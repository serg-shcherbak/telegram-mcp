import asyncio
import json
import os
from pathlib import Path
import re
import select
import socketserver
import stat
import subprocess
import time
import wave

MAX_SOURCE_BYTES = 256 * 1024 * 1024
MAX_PART_BYTES = 24_000_000
SAMPLE_RATE = 16000
MAX_SAMPLES = 14400 * SAMPLE_RATE
PART_PCM_BYTES = 600 * SAMPLE_RATE * 2
CONVERSION_TIMEOUT = 120
ACK_TIMEOUT = 180


class ConversionError(Exception):
    def __init__(self, message, reason="conversion_failed"):
        super().__init__(message)
        self.reason = reason


def convert_parts(folder, limit=MAX_PART_BYTES):
    """Yield contiguous sample ranges, retaining only the current WAV until next()."""
    source, output = folder / "source.bin", folder / "part.wav"
    if source.is_symlink() or not source.is_file() or source.stat().st_size > MAX_SOURCE_BYTES:
        raise ConversionError("Audio source exceeds the 256 MiB source limit.", "too_large")
    if type(limit) is not int or not 46 <= limit <= MAX_PART_BYTES:
        raise ConversionError("Invalid audio part byte limit.")
    pcm_limit = min(PART_PCM_BYTES, ((limit - 44) // 2) * 2)
    process = None
    try:
        process = subprocess.Popen(
            [
                "ffmpeg",
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-threads",
                "1",
                "-filter_threads",
                "1",
                "-filter_complex_threads",
                "1",
                "-protocol_whitelist",
                "file,pipe",
                "-i",
                str(source),
                "-map",
                "0:a:0",
                "-ac",
                "1",
                "-ar",
                str(SAMPLE_RATE),
                "-c:a",
                "pcm_s16le",
                "-f",
                "s16le",
                "pipe:1",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            env={"PATH": os.defpath, "LANG": "C", "LC_ALL": "C"},
        )
        start, index = 0, 0
        while True:
            deadline = time.monotonic() + CONVERSION_TIMEOUT
            written = 0
            with wave.open(str(output), "wb") as audio:
                os.chmod(output, 0o600)
                audio.setparams((1, 2, SAMPLE_RATE, 0, "NONE", "not compressed"))
                while written < pcm_limit:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not select.select([process.stdout], [], [], remaining)[0]:
                        raise ConversionError("Audio conversion timed out.", "converter_timeout")
                    chunk = os.read(process.stdout.fileno(), min(65536, pcm_limit - written))
                    if not chunk:
                        break
                    if start + (written + len(chunk)) // 2 > MAX_SAMPLES:
                        raise ConversionError(
                            "Audio exceeds the measured 14400 second duration limit.",
                            "duration_limit",
                        )
                    audio.writeframesraw(chunk)
                    written += len(chunk)
            if written % 2:
                raise ConversionError("Audio decoder returned incomplete PCM samples.")
            if written < pcm_limit:
                if process.wait(timeout=max(0.01, deadline - time.monotonic())) != 0:
                    raise ConversionError(
                        "Audio codec is unsupported or corrupt.", "unsupported_audio"
                    )
            if not written:
                break
            if output.stat().st_size > limit:
                raise ConversionError("Converted audio exceeds the actual part byte limit.")
            frames = written // 2
            yield {"index": index, "start_sample": start, "frames": frames}
            output.unlink(missing_ok=True)
            start += frames
            index += 1
            if written < pcm_limit:
                break
    except (OSError, subprocess.SubprocessError):
        raise ConversionError(
            "Audio conversion failed or temporary storage is unavailable."
        ) from None
    finally:
        if process is not None:
            if process.poll() is None:
                process.kill()
            process.wait()
            process.stdout.close()
        output.unlink(missing_ok=True)


async def receive_parts(root, job_id, limit):
    """Private Unix socket manifests; advancing ACK releases the previous part."""
    reader, writer = await asyncio.open_unix_connection(str(root / "converter.sock"))
    try:
        writer.write(
            (json.dumps({"job": job_id, "limit": limit}, ensure_ascii=False) + "\n").encode()
        )
        await writer.drain()
        expected_start, expected_index = 0, 0
        while True:
            line = await asyncio.wait_for(reader.readline(), CONVERSION_TIMEOUT + 5)
            if len(line) > 4096 or not line:
                raise ConversionError("Audio converter disconnected; conversion is incomplete.")
            result = json.loads(line)
            if result.get("status") == "complete":
                return
            if result.get("status") != "part":
                reason = result.get("reason")
                if reason not in {
                    "too_large",
                    "converter_timeout",
                    "duration_limit",
                    "unsupported_audio",
                }:
                    reason = "conversion_failed"
                raise ConversionError(
                    "Audio conversion failed; source is unsupported, corrupt, or over its limits.",
                    reason,
                )
            part = result["part"]
            if (
                part.get("index") != expected_index
                or part.get("start_sample") != expected_start
                or type(part.get("frames")) is not int
                or not 0 < part["frames"] <= PART_PCM_BYTES // 2
                or expected_start + part["frames"] > MAX_SAMPLES
            ):
                raise ConversionError("Audio converter returned discontinuous sample ranges.")
            path = root / job_id / "part.wav"
            if path.is_symlink() or not path.is_file() or not 44 < path.stat().st_size <= limit:
                raise ConversionError("Audio converter returned an invalid or oversized part.")
            with wave.open(str(path), "rb") as audio:
                if (
                    audio.getnchannels(),
                    audio.getsampwidth(),
                    audio.getframerate(),
                    audio.getnframes(),
                ) != (1, 2, SAMPLE_RATE, part["frames"]):
                    raise ConversionError("Audio converter returned incomplete part samples.")
            yield part, path
            expected_start += part["frames"]
            expected_index += 1
            writer.write(b"next\n")
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(ACK_TIMEOUT)
        parts = None
        try:
            line = self.rfile.readline(4097)
            if len(line) > 4096:
                return
            request = json.loads(line)
            job_id = request.get("job")
            if not isinstance(job_id, str) or not re.fullmatch("[a-f0-9]{32}", job_id):
                return
            folder = self.server.root / job_id
            if folder.is_symlink() or folder.resolve().parent != self.server.root.resolve():
                return
            parts = convert_parts(folder, request.get("limit"))
            for part in parts:
                self.wfile.write(
                    (
                        json.dumps({"status": "part", "part": part}, ensure_ascii=False) + "\n"
                    ).encode()
                )
                self.wfile.flush()
                if self.rfile.readline(16) != b"next\n":
                    return
            self.wfile.write(b'{"status":"complete"}\n')
            self.wfile.flush()
        except Exception as error:
            try:
                self.wfile.write(
                    (
                        json.dumps(
                            {
                                "status": "error",
                                "reason": (
                                    error.reason
                                    if isinstance(error, ConversionError)
                                    else "conversion_failed"
                                ),
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    ).encode()
                )
                self.wfile.flush()
            except OSError:
                pass
        finally:
            if parts is not None:
                parts.close()


def main():
    root = Path("/audio")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    address = root / "converter.sock"
    if address.exists():
        if not stat.S_ISSOCK(address.lstat().st_mode):
            raise SystemExit("Audio converter socket path is not a socket")
        address.unlink()
    os.umask(0o077)
    with socketserver.UnixStreamServer(str(address), _Handler) as server:
        server.root = root
        server.serve_forever()


if __name__ == "__main__":
    main()
