# Long-audio acceptance

## Implemented boundary

Direct `transcribe_voice` requests cover known short, supported audio. Long, unknown,
oversized or conversion-requiring inputs become persisted start/poll/cancel jobs with
one active operation. Each part and result rechecks permission and source identity.
Parts carry continuous PCM sample offsets; unknown provider outcomes and unprocessed
ranges remain explicit after restart. There is no automatic paid retry or queue.
A failed/expired identity does not restart on polling; cancellation does not reset it.

The isolated converter receives source/scratch files over a Unix socket, emits mono
16 kHz PCM WAV parts of at most ten minutes/24,000,000 bytes, and removes source/parts
after use. It has no network, credentials, Telegram session or host Docker socket.
The main process owns scratch outside allowed media roots. Limits are 256 MiB source,
four decoded hours and two hours job wall time. These are rejection boundaries, not
proof of live-provider acceptance at every maximum.

## Measured synthetic conversion, 10 November 2026

These measurements were supplied by the implementation run; originals were generated
tones, with no personal media, Telegram session or paid ASR calls.

| Environment and input | Observed result | What it establishes |
| --- | --- | --- |
| Generic capped VPS, two-hour Opus, 57,648,012 bytes | 79.017 s; ffmpeg RSS 55,504 KiB | Preparation alone can exceed a 55-second tool window |
| Same VPS, four-hour backpressure exercise | 212.491 s; ffmpeg RSS 55,944 KiB, Python RSS 20,236 KiB; peak disk 76,849,029 bytes | Bounded continuous conversion under this workload |
| Local production module, two-hour Ogg and 601.123-second MP3 | PCM matches independent full decode from the same decoder | Sample continuity and non-ten-minute tail coverage |
| Actual converter image, local Linux/aarch64, Unix socket, 384 MiB/no swap, 0.25 CPU, 64 PID, no network/read-only root | Two hours: 12 parts, 90.196 s, cgroup peak 67,702,784 bytes, source-plus-part disk 76,848,056 bytes; matching reference hash; scratch cleaned | Container/socket wire, continuous output and cleanup under these specific caps |

The 55-second server tool window and 120-second tunnel absolute deadline motivate
persisted jobs; progress cannot extend the latter. The VPS benchmark is not a
minimum-host specification, and the local VM is not the production host. Reference
hash equality uses the same decoder and establishes continuity, not independent
codec correctness. No provider latency, speech-recognition accuracy, live 256 MiB
upload, two-hour paid job or arbitrary-codec success was measured.

## Release checks still required

Run the frozen full tests, packaging scans, isolated non-editable installation and
both exact-commit image builds. Inspect actual container identity, network, caps,
mounts and cleanup. The fork's real Actions runs must be discovered and pass; an
empty check list or local tests do not establish CI success. The calling release
workflow owns those checks and records their observed results separately.

A later owner-authorized provider check must record source type/duration/size, model,
coverage, offsets, actual engine/fallback, latency, cancellation/restart behavior and
charges without placing media, credentials or transcript content in Git. Synthetic
conversion proof does not authorize that external transmission.
