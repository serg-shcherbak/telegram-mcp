# Original attachment acceptance

## Current result

The local candidate returns an original as an MCP embedded binary resource over the
authenticated private connection. Actual Dot materialization and format tasks are
**not accepted**. A server path, a displayed URI, an SDK round trip, or a preview
does not establish that Dot can open the original.

The existing My Telegram connection exposes six older tools and cannot register
this candidate or a staging connection. Computer Use refused access to the Dot
application for safety reasons. That refusal was not bypassed. A separate supported
Dot staging connection is the missing prerequisite. Production, its credentials,
and the real Telegram account have not been changed.

## Candidate boundary

- `export_attachment` accepts a chat, message, and optional media identity. It
  accepts no filesystem path. Read and attachment scopes must both permit the
  resolved canonical source.
- The returned metadata identifies the source, original size, and SHA-256. Media
  identities remain strings, preserving Telegram's 64-bit values.
- The binary resource is delivered inline. Its `telegram://` URI is an identity,
  not a public download endpoint or a later retrieval capability. No retained
  handle exists to expire. Already delivered bytes cannot be recalled.
- The provisional local ceiling is 6,000,000 original bytes. The complete response
  must fit the tunnel's documented 10 MiB serialized envelope. This ceiling is not
  a measured host capability or the product's final large-file limit.
- Downloaded originals are temporary. Oversize input, interrupted transfer,
  cancellation, changed source, and revoked access must fail without retaining
  the original or returning it as complete.

The tunnel documentation specifies a 120-second absolute deadline, including queue
time; progress does not extend it. Neither real tunnel forwarding nor timing under
load has been established in this run. The installed tunnel-client executable
supports origin-bound `--mcp.extra-headers` and `--mcp.discovery-extra-headers` with
`env:`/`file:` values. No upgrade or production configuration change follows from
that observation. [Tunnel limits](https://developers.openai.com/api/docs/guides/secure-mcp-tunnels#limits),
[installed revision configuration](https://github.com/openai/tunnel-client/blob/c2144b0904980bcbae2636cdca4f4167e9818e0b/docs/configuration.md),
[MCP binary resources](https://modelcontextprotocol.io/specification/2025-11-25/server/resources).

## Synthetic staging procedure

Use a separate test connection. Do not attach this server to the existing personal
Telegram connection or copy its session, credentials, or environment. The helper
uses a fake Telegram source and freshly generated local credentials. It performs
no Telegram login and no external transcription.

From a disposable checkout with the frozen development environment installed:

```sh
uv run --frozen python tests/manual_dot_originals.py --port 8766
```

The helper binds only `127.0.0.1`, creates a private temporary directory, and prints
paths to its credential file and local answer manifest. Keep that directory local.
Run `--self-check` first for the same fake sources through the real local MCP SDK;
this mode finishes without opening a listening socket. Only `get_messages` and
`export_attachment` are supported by the synthetic adapter; the normal safe-profile
read catalogue is retained and other demo reads can fail explicitly. The normal runtime can append
synthetic diagnostics to the ignored checkout `mcp_errors.log`.
Configure a separate supported private tunnel/connection to its `/mcp` endpoint,
using the file credential as `x-dot-backend-token` for both discovery and MCP
requests. This step must use the owner's supported Dot connection flow; the
current connector provides no creation API. The local helper does not create a
tunnel or register itself with Dot.

Keep the answer manifest outside Dot's context until answers have been recorded.
Request the neutral source catalogue with `get_messages` for synthetic chat `1`,
then request each original with `export_attachment`. Record the exact tool result,
whether a usable original is materialized, the task answer, and coverage. Compare
the materialized file's hash independently where the host permits it. If the host
does not expose a verifiable materialized original, record that limitation rather
than accepting a URI or text description as byte evidence.

The fixture manifest supplies the precise source identities and answer key for:

| Fixture | Task and required evidence | Local transfer | Dot original | Dot task |
| --- | --- | --- | --- | --- |
| Image | Read marker and identify colored regions | Exact bytes verified | Not tested | Not tested |
| Text PDF | Read the embedded text marker | Exact bytes verified | Not tested | Not tested |
| Scanned PDF | Read the visible marker without a text layer | Exact bytes verified | Not tested | Not tested |
| DOCX | Identify the item and quantity in its table | Exact bytes verified | Not tested | Not tested |
| XLSX | Inspect both sheets; distinguish formula from cached value | Exact bytes verified | Not tested | Not tested |
| WAV | Identify the synthetic tone and its covered duration | Exact bytes verified | Not tested | Not tested |
| MP4 | Identify the color sequence and time ranges | Exact bytes verified | Not tested | Not tested |
| Album sources | Preserve shared group and individual source identities | Exact bytes verified | Not tested | Not tested |

The synthetic tone tests original delivery and playback only. It supplies no speech
recognition, provider, ordinary-audio transcription, or long-audio evidence.
Fixture-generation checks may inspect the synthetic PDF/XML/media to validate the
answer key; the production server does not parse documents, run OCR, or analyze
video.

Preserve non-secret evidence before stopping the helper with Ctrl-C; its newly
allocated staging directory is removed on exit. Revoke/remove the separate staging
connection and its tunnel. Do not change the production connection.

## Acceptance record

Keep four evidence classes separate: Telegram-adapter download, real local MCP
serialization, actual Dot materialization, and Dot task answers. For each format
record the candidate commit, dependency versions, source identity, original and
received hashes, serialized response size, elapsed time, and observed limitations.
Do not place the staging credential, personal messages, or session material in Git.

The remaining direct-audio, long-recording, clean-install, image-build, and release
work depends on successful actual-host acceptance. If the host rejects the binary
resource or a requested format, preserve the exact refusal and return the blocker.
Changing the host transfer mechanism or the product acceptance scope requires a
new explicit decision. No public media route or server parser is an automatic
fallback.

## Local measurements, 10 November 2026

The owner-run helper was independently rerun with the actual authenticated MCP SDK
and fake Telegram adapter. All seven original hashes matched and temporary media was
empty afterward. These are local ASGI measurements, not network, tunnel, Dot, or
resource-load acceptance. Result sizes below exclude the outer JSON-RPC envelope.

| Original | Raw bytes | CallToolResult JSON bytes | Local elapsed ms |
| --- | ---: | ---: | ---: |
| colors.png | 2203 | 3391 | 161.3 |
| contract.pdf | 966 | 1749 | 13.68 |
| scan.pdf | 6634 | 9309 | 19.62 |
| parts.docx | 889 | 1759 | 11.97 |
| orders.xlsx | 1761 | 2909 | 11.21 |
| tone.wav | 16044 | 21839 | 12.37 |
| rgb.mp4 | 1850 | 2913 | 11.11 |

A separate SDK regression also transfers the full provisional 6,000,000-byte
original and checks that the result remains below 10 MiB. No larger-file or
real-tunnel claim follows from that test. The tone is not speech; the video is
three seconds of generated colors.
