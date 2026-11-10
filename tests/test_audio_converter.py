"""Synthetic PCM proves converter byte bounds and continuous sample offsets."""

import hashlib
import json
import socket
import threading
from types import SimpleNamespace
import wave

import pytest

from telegram_mcp import audio_converter as converter


@pytest.fixture
def decoder(tmp_path, monkeypatch):
    processes = []

    def install(pcm, status=0):
        raw = tmp_path / "decoded.pcm"
        raw.write_bytes(pcm)

        def start(*args, **kwargs):
            assert set(kwargs["env"]) <= {"PATH", "LANG", "LC_ALL"}
            process = SimpleNamespace(stdout=raw.open("rb"), done=False, killed=False)

            def wait(**kwargs):
                process.done = True
                return status

            process.wait = wait
            process.poll = lambda: status if process.done else None
            process.kill = lambda: setattr(process, "killed", True)
            processes.append(process)
            return process

        monkeypatch.setattr(converter.subprocess, "Popen", start)
        return processes

    return install


def source(tmp_path):
    folder = tmp_path / ("a" * 32)
    folder.mkdir()
    (folder / "source.bin").write_bytes(b"synthetic compressed input")
    return folder


def test_parts_preserve_exact_pcm_order_offsets_and_actual_limits(tmp_path, decoder):
    pcm = bytes(range(22))
    decoder(pcm)
    folder = source(tmp_path)
    reconstructed = bytearray()
    manifests = []
    for part in converter.convert_parts(folder, 52):
        manifests.append(part)
        output = folder / "part.wav"
        assert output.stat().st_size <= 52
        with wave.open(str(output), "rb") as audio:
            assert audio.getnframes() == part["frames"]
            reconstructed.extend(audio.readframes(audio.getnframes()))
    assert hashlib.sha256(reconstructed).digest() == hashlib.sha256(pcm).digest()
    assert [(p["start_sample"], p["frames"]) for p in manifests] == [(0, 4), (4, 4), (8, 3)]
    assert not (folder / "part.wav").exists()


def test_corrupt_codec_cleans_output_without_provider_payload(tmp_path, decoder):
    decoder(b"", status=1)
    folder = source(tmp_path)
    with pytest.raises(converter.ConversionError, match="unsupported or corrupt"):
        list(converter.convert_parts(folder, 52))
    assert not (folder / "part.wav").exists()


def test_cancellation_closes_decoder_and_removes_part(tmp_path, decoder):
    processes = decoder(b"x" * 32)
    folder = source(tmp_path)
    parts = converter.convert_parts(folder, 52)
    next(parts)
    parts.close()
    assert processes[0].stdout.closed
    assert processes[0].killed
    assert not (folder / "part.wav").exists()


def test_converter_timeout_kills_decoder_and_cleans_part(tmp_path, decoder, monkeypatch):
    processes = decoder(b"x" * 32)
    monkeypatch.setattr(converter.select, "select", lambda *args: ([], [], []))
    folder = source(tmp_path)
    with pytest.raises(converter.ConversionError, match="timed out"):
        list(converter.convert_parts(folder, 52))
    assert processes[0].stdout.closed
    assert processes[0].killed
    assert not (folder / "part.wav").exists()


def test_disk_full_cleans_part_and_decoder(tmp_path, decoder, monkeypatch):
    processes = decoder(b"x" * 32)
    folder = source(tmp_path)

    def full(*args):
        raise OSError("private source path must not escape")

    monkeypatch.setattr(converter.wave, "open", full)
    with pytest.raises(converter.ConversionError, match="temporary storage"):
        list(converter.convert_parts(folder, 52))
    assert processes[0].stdout.closed and processes[0].killed
    assert not (folder / "part.wav").exists()


def test_wire_disconnect_stops_decoder_and_removes_current_part(tmp_path, decoder):
    processes = decoder(b"x" * 32)
    folder = source(tmp_path)
    server, client = socket.socketpair()
    thread = threading.Thread(
        target=converter._Handler, args=(server, None, SimpleNamespace(root=tmp_path))
    )
    thread.start()
    stream = client.makefile("rb")
    try:
        client.sendall((json.dumps({"job": folder.name, "limit": 52}) + "\n").encode())
        assert json.loads(stream.readline())["status"] == "part"
        assert (folder / "part.wav").exists()
    finally:
        stream.close()
        client.close()
        thread.join(timeout=2)
        server.close()
    assert not thread.is_alive()
    assert processes[0].stdout.closed and processes[0].killed
    assert not (folder / "part.wav").exists()


def test_duration_limit_is_an_error_and_never_a_silent_trim(tmp_path, decoder, monkeypatch):
    decoder(b"x" * 32)
    monkeypatch.setattr(converter, "MAX_SAMPLES", 5)
    folder = source(tmp_path)
    parts = converter.convert_parts(folder, 52)
    assert next(parts)["frames"] == 4
    with pytest.raises(converter.ConversionError, match="duration limit"):
        next(parts)
    assert not (folder / "part.wav").exists()


def test_converter_ack_wait_covers_two_attempts_within_owner_job_budget():
    from telegram_mcp import transcription_jobs

    assert converter.ACK_TIMEOUT >= transcription_jobs.MAX_JOB_SECONDS
