# ASR retry contract update, 2026-11-10

This records the owner's newer direction and overrides the no-automatic-repost
part of R12/KTD6 in the original direct-task plan. It does not reopen that plan.

- After the first private ASR provider failure, immediately retry the same audio
  once, including an unknown first POST outcome. The owner accepted duplicate cost.
- After a second failure, stop and report it to the owner in Dot with available
  partial text. Do not add a third automatic request or native fallback.
- For long audio, this budget applies to the failing part. Preserve prior parts.
  Polling, concurrent requests and process restart must not silently replenish it.
- Cancellation, changed source, revoked access, invalid configuration and local
  resource limits do not authorize another provider upload.
- Further attempts after exhaustion require a fresh explicit owner task through
  Dot. No server database edits or operator reset should be needed. Expired text
  must not permanently prevent a newly requested transcription.
- Keep one-file direct ASR for supported known-duration recordings up to 600s and
  24,000,000 bytes. Long interviews/calls use internal parts up to 600s each,
  preserving order and offsets. These internals should not burden the user.

Use synthetic tests. No real provider upload, personal audio or Telegram send is
authorized for verification. Existing deployment authorization remains conditional
on readiness; source and permission checks remain mandatory before any retry.
