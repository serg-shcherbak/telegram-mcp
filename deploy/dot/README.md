# Private Dot installation

One owner runs one installation with their own existing Telegram file session. The
server reads permitted Telegram content, returns originals and transcribes audio;
Dot analyzes documents, images, tables and video. Observation is off. Each send
requires Dot's confirmation of the exact recipient, text and reply target.

The profile is a release candidate. Small synthetic originals passed actual Dot
acceptance. A release also needs successful checks on its exact commit: frozen
installation, packages, both images, isolated smoke checks and the fork's actual
GitHub Actions runs. An empty checks list is not a passing result. Deployment is a
separate operator action; do not replace an existing service while checks are pending.

## Supported build

The supported private profile uses Linux containers, Python **3.13.12**, uv **0.11.3**
and the committed `uv.lock`. Its setuptools backend is pinned in `pyproject.toml`
and the development lock. Do not regenerate the lock while installing a release.
Python dependencies are frozen; Debian image tags and apt repositories are not a
bit-for-bit OS lock. Retain both resulting image IDs/digests for exact updates and
rollback, along with the reviewed source commit. Never install the unrelated PyPI
package by running `pip install telegram-mcp`.

From a checkout of the reviewed full commit, build the two images:

```sh
DOT_REV=$(git rev-parse HEAD)
uv sync --frozen --python 3.13.12
uv run --frozen --no-sync uv build --offline --no-build-isolation --out-dir /tmp/dot-dist
docker build -f deploy/dot/Dockerfile --build-arg VCS_REF="$DOT_REV" -t "telegram-dot:$DOT_REV" .
docker build -f deploy/dot/Converter.Dockerfile --build-arg VCS_REF="$DOT_REV" -t "telegram-dot-converter:$DOT_REV" .
```

No Events extra is installed. The safe HTTP transport requires backend authentication
on discovery and every request, independently of the tunnel. CI builds these private
Dockerfiles, not the legacy root Dockerfile or generic Compose definition.

For a separate local install, place the environment outside the checkout and install
without editable imports. Keep the source checkout for the provenance check:

```sh
export UV_PROJECT_ENVIRONMENT=/tmp/dot-installed
uv sync --frozen --python 3.13.12 --no-dev --no-editable
"$UV_PROJECT_ENVIRONMENT/bin/python" -I tests/smoke_dot_install.py "$PWD/deploy/dot/dot_server.py"
```

The smoke helper uses fresh synthetic configuration and an empty local session. It
checks installed imports, safe startup, real MCP initialization, authentication and
revocation without connecting to Telegram or an ASR provider. Container CI runs the
same helper with no network and no host private mounts. These checks do not replace
an owner's real connection acceptance.

## Private host layout

Keep all operating values outside the checkout, image context and allowed media root.
Use a dedicated absolute host directory, for example `/srv/telegram-dot-private`, with
separate `session`, `config` and `state` directories, plus `state/audio`. Create these
before Compose starts: directories mode 0700, files mode 0600, runtime UID/GID 1000.
The runtime needs write access to the session SQLite file/sidecars and state.

Copy `deploy/dot/.env.example` to a private environment file and
`deploy/dot/access.example.json` to `config/access.json`. Edit these data files;
no Python edits are required. Replace the API placeholders, the positive numeric
`owner_id`, and the backend token. Generate a unique token of at least 32 printable
ASCII characters without whitespace or commas, for example with
`openssl rand -hex 32`. Never print the completed configuration or commit it.
The shipped token placeholder intentionally fails closed.

Use the existing owner's native SQLite session as `session/telegram.session`.
Stop the process using that authorization **before** moving it or starting Dot;
never run a second client with the same authorization key, even on another host.
Never borrow a neighboring service's session. Preserve the stopped SQLite file and
any sidecars together. The container paths are `/app/session/telegram.session`,
`/app/config/access.json` and writable `/app/state`. Code lives in `/opt`.

If no authorized session exists, run `telegram-mcp-generate-session --qr` (or
`--phone`) locally with the owner's Telegram API credentials. It creates a native
session under `${XDG_STATE_HOME:-~/.local/state}/telegram-mcp/sessions/`, protecting
the directory/file before any authorization write; existing destinations and SQLite
sidecars are refused. It prints the path, never a serialized session, and does not
rewrite `.env`. POSIX permissions are required; native Windows authorization creation
remains unavailable until its ACL protection is verified.

For an existing StringSession, stop its server and run
`telegram-mcp-migrate-session` locally. It reads the existing environment, writes a
private native session and verifies it. `--target` selects a private destination
outside checkout and media directories. Failure/cancellation removes the new target
and preserves the source. Use the printed session-name setting, remove the old string
variable and keep only the single owner session in this profile.

Private sessions, credentials, transcript databases, audio scratch space, logs,
caches and backups must never enter Git, bundles, images or shared build contexts.
Artifact tests seed synthetic private files at multiple depths and inspect actual
wheel, sdist, MCP bundle and Docker context output. Exclusions are a second boundary;
keep real private material outside the checkout even if it would be ignored.

## Permissions and ASR consent

Omitting `read` limits it to the owner's Saved Messages. Add canonical marked numeric
chat IDs only for the desired scope: positive users, negative basic groups and
`-100...` channels/supergroups. `read`, `send`, `attachments` and `external_asr` are
independent; literal `"all"` explicitly chooses all chats for that operation. Send,
attachment and external ASR scopes start empty. `TELEGRAM_ALLOWED_CHAT_IDS`, if set,
is an additional ceiling. Reading does not mark messages read.

Adding chats to `external_asr` is the owner's explicit setup opt-in for direct OpenAI
ASR in those chats. Audio leaves Telegram and this installation for OpenAI; API use
can incur provider charges. Budget depends on model, audio duration and current
provider pricing. A key alone is not consent. Reading history and background events
do not trigger external ASR. `transcribe_voice` requires read and external ASR scope,
independently of attachment export; the default model is `gpt-transcribe` at the
official `https://api.openai.com/v1` endpoint. Leave the API key blank and scopes empty
until the owner opts in. Native Telegram fallback applies only to voice/video notes,
not ordinary audio files. Results identify engine, fallback and completeness.

Known short audio (at most 600 seconds, supported codec and at most 24,000,000 bytes)
can use the direct path. Long/unknown duration, oversized or other-codec audio uses a
persisted job through the same `transcribe_voice` tool: start, poll with the same
source identity, or cancel with `cancel=True`. One operation runs at a time; there is
no queue. Conversion uses an isolated ffmpeg sidecar over a Unix socket. It receives
only audio scratch files, no Telegram session, credentials, access configuration or
provider key. The main server has no Docker socket. Bounds are 256 MiB source,
four decoded hours and two hours job wall time; each WAV part is at most ten minutes
and 24,000,000 bytes. Status exposes sample rate, offsets, completed/unknown ranges,
unprocessed time and partial coverage. Do not present partial text as a full transcript.

Permission and source identity are checked before each provider part and each result.
An uncertain paid request is recorded as unknown and is never retried automatically,
including after restart. A terminal failed/expired job does not restart on polling the
same identity; deliberate operator reset or a new source identity is required.
Cancellation is not a retry/reset command. See [long-audio verification](../../docs/acceptance/long-audio.md)
for measured coverage and the limits of synthetic evidence. No live provider accuracy,
latency or maximum-source acceptance is claimed.

Transcript identity includes account, source version, provider/model and material
options. Default text retention is seven days. Expiration is applied on access, not by
a background purge timer. With `TELEGRAM_TRANSCRIPT_RETENTION_SECONDS=0`, direct cache
storage is disabled and job text stays only in memory until retrieval, the next new
job or restart; job metadata/tombstones remain. Cache/job databases are plaintext in
private state outside allowed media roots. Keep them and all backups private.

## Start only the selected installation

Set these selectors in the operator's shell or a separate private Compose environment
file. Use the **same project name and paths** for future updates:

```sh
export TELEGRAM_DOT_IMAGE="telegram-dot:$DOT_REV"
export TELEGRAM_DOT_CONVERTER_IMAGE="telegram-dot-converter:$DOT_REV"
export TELEGRAM_DOT_ENV_FILE=/srv/telegram-dot-private/telegram.env
export TELEGRAM_DOT_SESSION_DIR=/srv/telegram-dot-private/session
export TELEGRAM_DOT_CONFIG_DIR=/srv/telegram-dot-private/config
export TELEGRAM_DOT_STATE_DIR=/srv/telegram-dot-private/state
docker compose -p telegram-dot -f deploy/dot/compose.yaml config --quiet
```

Compose refuses absent host directories, publishes only `127.0.0.1:8765`, and runs
both services as UID/GID 1000 with 384 MiB memory/no swap, 0.25 CPU and 64 PID caps.
The converter has no network, a read-only root and only the audio bind mount. The
main root remains writable for compatibility; its host writable mounts are state and
session, while configuration is read-only. Resource caps are configuration, not a
minimum-VPS performance guarantee. Check available host capacity and loopback port
without stopping or recreating neighboring services.

After the exact release passes its checks and deployment is authorized, start this
project with `docker compose -p telegram-dot -f deploy/dot/compose.yaml up -d`.
Do not run generic root Compose commands or broadly stop containers. Configure the
trusted private client/tunnel to forward `x-dot-backend-token` to this backend origin
for both discovery and MCP requests; use origin-bound `file:`/`env:` header forwarding.
The tunnel credential and backend token are separate. Verify missing/wrong backend
credentials fail even through the tunnel. Do not expose the backend publicly.

## Revoke, update and roll back

Remove a chat from the relevant scopes to revoke access; remove/rotate the profile
token to revoke a client. Tokens and scopes are reread on requests. Owner/media-root
changes require restart. Already delivered originals/text cannot be recalled. Rotate
provider credentials at the provider and update the private environment; restart this
project to load the environment. Revoke compromised Telegram authorization in Telegram
Settings > Devices, then stop the affected process and perform owner-authorized setup.

Before updating, record the exact current source commit, both image IDs/digests,
Compose project/paths and compatible state version. Stop only this project, then make
a private, permissions-preserving backup of its stopped session (including sidecars),
access configuration and state. Keep the backup outside the checkout and media root.
Do not delete volumes or overwrite the old images. Review changes and run the checks
on the new full commit, build both images and change only this project's image
selectors. Validate Compose quietly and start the same project; verify the configured
owner, authenticated discovery, scoped read and approved synthetic original before
using personal tasks. Do not create another Telegram client as a health probe.

Rollback selects the previously recorded pair of images and its compatible configuration
and state, using the same project/paths. Stop the new version before restoring stopped
state; preserve the current state copy for investigation. Before rolling back to a
version without persisted-job support, set transcription `off` and empty external ASR
scope, preserve the job database and source identities, and resolve recorded/unknown
paid work deliberately. Blindly restoring an earlier database or reposting parts can
charge twice and lose the record of uncertain work. A compatible code rollback does
not imply that an older state format is safe. Verify the same limited connection
checks before reenabling ASR; observation stays off.
