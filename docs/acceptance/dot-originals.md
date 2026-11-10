# Original attachment acceptance

## Current result

The private original-transfer gate passed on 10 November 2026 for all seven small
synthetic fixtures. The owner reported actual Dot materialization and correct
known-answer tasks for PNG, text PDF, scanned PDF, DOCX, XLSX, WAV and MP4, including
album relationships and video time ranges. The owner also reports hashing the six
document/audio/video originals from received bytes, matching export metadata.
The agent subsequently retrieved all seven through the actual test-plugin/tunnel
route, materialized and hashed the response bytes, and verified their structure.
These are separate observations. Neither establishes arbitrary-size/codec support,
real Telegram storage integrity, external ASR, or release readiness.

The owner created the separate synthetic plugin/tunnel using the supported flow.
Computer Use's earlier refusal was not bypassed. The real My Telegram installation,
its configuration, and its Telegram account have not been changed. Server document
parsers, OCR, video analysis and a public media route were not added; analysis ran
on the Dot/client side. The reported hashes validate transport relative to plugin
metadata, not independent Telegram storage authenticity.

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
| Image | Read marker and identify colored regions | Exact bytes verified | Opened, owner report | Marker and colors match |
| Text PDF | Read the embedded text marker | Exact bytes verified | Opened, owner report | Code, both pages and final total match |
| Scanned PDF | Read the visible marker without a text layer | Exact bytes verified | Opened, owner report | Code and total match; client OCR/visual check |
| DOCX | Identify the item and quantity in its table | Exact bytes verified | Opened, owner report | Code and table match; client structure/library analysis |
| XLSX | Inspect both sheets; distinguish formula from cached value | Exact bytes verified | Opened, owner report | Sheets, formula and saved value match; no Excel recalculation |
| WAV | Identify the synthetic tone and its covered duration | Exact bytes verified | Opened, owner report | Duration, tone frequency and absence of speech match |
| MP4 | Identify the color sequence and time ranges | Exact bytes verified | Opened, owner report | Duration, sampled colors and absence of audio track match |
| Album sources | Preserve shared group and individual source identities | Exact bytes verified | Retrieved in Dot | Messages 1 and 3 in album 901 match |

The synthetic tone tests original delivery and playback only. It supplies no speech
recognition, provider, ordinary-audio transcription, or long-audio evidence.
Fixture-generation checks may inspect the synthetic PDF/XML/media to validate the
answer key; the production server does not parse documents, run OCR, or analyze
video.

On 10 November the owner pasted Dot's result: the original image opened, the code
was `IMAGE-482`, and the regions were red, green, blue from left to right. The prompt
did not disclose those answers; neutral source metadata does not contain the code.
The fixture source independently agrees. The active test tunnel subsequently showed
14 polled commands with an empty queue and healthy readiness. Those aggregate
counters corroborate connection use, not the exact image call or a host-file hash.
At that first checkpoint the original Dot tool trace/materialized-file hash was not
supplied to this chat. A later owner report covers the remaining formats and reports
computed byte hashes; the literal hash strings were not pasted. The subsequently
available test-plugin tools let the agent independently record all seven actual
EmbeddedResource byte hashes and match the controlled contents. The per-file
results and qualification are retained in the workspace evidence. No real account
or personal media was used.

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
