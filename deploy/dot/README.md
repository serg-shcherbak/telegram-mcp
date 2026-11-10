# Private Dot installation definition

This is the direct-task installation definition under development. It has not passed
image-build, Dot original-file acceptance, or release checks. Do not replace an existing
service with it until those checks pass and deployment is authorized. Observation is off.
The server supplies permitted Telegram operations; Dot analyzes originals. Original export
and the new direct ASR interface are not exposed yet.

Use one existing own Telegram file session. Stop the process using that authorization
before migration or startup; never run a second client with the same authorization key.
Do not rerun login when an existing authorized session is available. Never borrow a session
from a neighboring service.

If a new login is needed, run `telegram-mcp-generate-session --qr` (or `--phone`) locally
with your Telegram API credentials. It creates a native SQLite session under
`${XDG_STATE_HOME:-~/.local/state}/telegram-mcp/sessions/`, with a private directory and 0600
file before any authorization write. Labels produce matching `TELEGRAM_SESSION_NAME_<LABEL>`
instructions. Existing destinations and SQLite sidecars are refused. The command prints
the path, never a serialized session, and does not rewrite `.env`.
Private file creation currently requires POSIX permissions. Native Windows setup fails
before creating authorization material until its ACL protection is verified; console QR
rendering and existing legacy core configurations remain available.

For an existing StringSession, stop its server and run `telegram-mcp-migrate-session`
locally. It reads the existing environment without changing it, writes a private native
session, and verifies that session. `--account work` selects labeled variables; `--target`
selects another private destination outside the checkout and allowed media directories.
On failure or cancellation the new target is removed and the source is preserved. Set the
printed session-name variable and remove the old string variable before restarting.

Keep the environment file, `access.json`, and session outside the checkout, image build
context, and media directory. Use owner-only 0700 host directories and 0600 files owned by
the runtime UID/GID 1000. Preserve the SQLite file and any existing sidecars together when
moving a stopped session. The container uses `/app/session/telegram.session`,
`/app/config/access.json`, and writable `/app/state`; code and the entrypoint live in `/opt`.
The root filesystem remains writable because existing runtime paths have not yet been
validated for a read-only root. Only `/app/state` and `/app/session` require writable mounts.

Copy `.env.example` to a private environment file and replace its placeholders. Copy
`access.example.json` to the private configuration directory, replace `owner_id` with your
positive numeric Telegram user ID, and replace `token` with a unique secret of at least 32
printable ASCII characters without whitespace or commas. The shipped placeholder is
intentionally invalid, so unedited setup cannot authenticate. Do not print or commit tokens.

Omitting `read` limits it to the owner's Saved Messages. Add canonical marked numeric chat
IDs only for the intended scope. Positive IDs identify users, negative IDs identify groups,
and `-100...` marked IDs identify channels/supergroups. Each of `read`, `send`, `attachments`,
and `external_asr` is independent; the literal `"all"` is an explicit all-chats choice for
that operation. Send, attachment, and external ASR scopes are empty by default. An optional
`TELEGRAM_ALLOWED_CHAT_IDS` remains an independent ceiling on each operation; use canonical
numeric IDs for this Dot profile. Dot must confirm the exact recipient, text, and reply
target before every send. There is no separate server approval channel.

Adding chats to `external_asr` is the owner's explicit opt-in for future direct OpenAI ASR
requests in those chats. Audio will leave Telegram and this installation for OpenAI, and
API use can incur charges. An API key alone is not consent. Reading history and background
events do not grant or trigger external ASR. The new direct ASR tool is not yet exposed;
this example keeps transcription off and includes no API key. Remove scopes to revoke
permission; remove or replace a profile token to revoke its client. Current token/scope
changes are reread, while owner/media-root changes require a restart. Revoke a compromised
Telegram authorization in Telegram Settings > Devices.

Configure the trusted private client to send `x-dot-backend-token` to this MCP origin. Use
the tunnel's origin-bound header facility if available; verify forwarding before use.
Do not expose the backend directly to the public internet. Compose publishes only host
loopback and keeps the documented 384 MiB memory/no-swap, 0.25 CPU, and 64 PID limits.

Build from the repository root with the frozen lock and this Dockerfile, supplying the
reviewed commit as `VCS_REF`. No Events extra is installed. Set `TELEGRAM_DOT_IMAGE` to that
immutable image reference and set the absolute host paths `TELEGRAM_DOT_ENV_FILE`,
`TELEGRAM_DOT_SESSION_DIR`, `TELEGRAM_DOT_CONFIG_DIR`, and `TELEGRAM_DOT_STATE_DIR` before using
this Compose file. The directories must already exist; Compose must not create them as
root. This definition does not start, update, or stop any service by itself. Image build,
release, and deployment validation remain separate outstanding work.
