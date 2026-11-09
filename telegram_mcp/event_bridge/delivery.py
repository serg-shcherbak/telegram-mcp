from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime

from .callbacks import CallbackError, encode_body, signed_headers
from .policies import PolicyScheduler, ReminderPolicy
from .protocol import ATTENTION, HEALTH, canonical
from .store import DeliveryRequest, _plain as plain
from hashlib import sha256


class DeliveryWorker:
    def __init__(self, service, *, owner_id, clock=None):
        self.service, self.store, self.owner_id = service, service.store, owner_id
        self.clock = clock or service.clock

    def flush(self):
        for watch in self.store.list(self.owner_id):
            grants = self.service.subscriptions(self.owner_id, watch.watch_id)
            if not grants:
                continue
            for grant in grants:
                self.store.dispatch_replays(self.owner_id, grant["id"], now=self.clock())
            PolicyScheduler(self.store, owner_id=self.owner_id, clock=self.clock).flush(
                watch.watch_id, self.service.activity_requests(self.owner_id, watch.watch_id)
            )
            for job in ReminderPolicy(self.store, owner_id=self.owner_id, clock=self.clock).due(
                watch.watch_id
            ):
                requests = tuple(
                    DeliveryRequest(g["recipient"], g["id"])
                    for g in grants
                    if g["name"] == ATTENTION
                )
                self.store.dispatch_control(
                    self.owner_id,
                    watch.watch_id,
                    watch.epoch,
                    job.job_id,
                    dict(
                        recheck_required=True,
                        job_id=job.job_id,
                        obligation_id=job.obligation_id,
                        scope_version=watch.active.revision,
                    ),
                    requests,
                    now=self.clock(),
                )
            health = self.store.health(self.owner_id, watch.watch_id)
            requests = tuple(
                DeliveryRequest(g["recipient"], g["id"]) for g in grants if g["name"] == HEALTH
            )
            payload = dict(
                status=health.status, reason=health.reason, coverage_gaps=health.gap_count
            )
            event_id = sha256(
                canonical((self.owner_id, watch.watch_id, watch.epoch, payload)).encode()
            ).hexdigest()
            self.store.dispatch_control(
                self.owner_id,
                watch.watch_id,
                watch.epoch,
                event_id,
                payload,
                requests,
                now=self.clock(),
            )

    async def run_once(self, *, limit=20):
        self.flush()
        for delivery in self.store.claim_due(self.owner_id, now=self.clock(), limit=limit):
            grant = self.service.get_subscription(self.owner_id, delivery.subscription_id)
            if (
                not grant
                or grant["epoch"] != delivery.epoch
                or grant["recipient"] != delivery.recipient_id
            ):
                self.store.retry(
                    self.owner_id,
                    delivery.delivery_id,
                    delivery.claim_token,
                    due_at=self.clock(),
                    dead=True,
                )
                continue
            if grant["name"] == ATTENTION:
                current_jobs = {
                    j.job_id
                    for j in ReminderPolicy(
                        self.store, owner_id=self.owner_id, clock=self.clock
                    ).due(delivery.watch_id)
                }
                if delivery.payload.get("job_id") not in current_jobs:
                    self.store.retry(
                        self.owner_id,
                        delivery.delivery_id,
                        delivery.claim_token,
                        due_at=self.clock(),
                        dead=True,
                    )
                    continue
            if grant["name"] == HEALTH:
                health = self.store.health(self.owner_id, delivery.watch_id)
                current = dict(
                    status=health.status, reason=health.reason, coverage_gaps=health.gap_count
                )
                if plain(delivery.payload) != current:
                    self.store.retry(
                        self.owner_id,
                        delivery.delivery_id,
                        delivery.claim_token,
                        due_at=self.clock(),
                        dead=True,
                    )
                    continue
            meta = self.store.event_metadata(self.owner_id, delivery.event_id)
            data = plain(delivery.payload)
            data.update(
                watch_id=delivery.watch_id,
                scope_version=grant["arguments"]["scope_version"],
                epoch=delivery.epoch,
            )
            body = encode_body(
                dict(
                    eventId=delivery.event_id,
                    name=grant["name"],
                    timestamp=meta["created"],
                    data=data,
                    cursor=self.service.cursor(grant, meta["seq"]),
                )
            )
            previous = (
                grant.get("previous_secret")
                if grant.get("rotation_until")
                and datetime.fromisoformat(grant["rotation_until"]) > self.clock()
                else None
            )
            headers = signed_headers(
                delivery.event_id,
                grant["id"],
                body,
                grant["secret"],
                previous_secret=previous,
                timestamp=int(self.clock().timestamp()),
            )
            # No await separates the final current-grant/epoch check from starting POST.
            if not self.store.reserve_delivery_retry(
                self.owner_id, delivery.delivery_id, delivery.claim_token, now=self.clock()
            ):
                continue
            if not self.store.delivery_current(
                self.owner_id, delivery.delivery_id, delivery.claim_token
            ):
                continue
            now = self.clock()
            response = None
            try:
                response = await self.service.callbacks.post(grant["url"], body, headers)
                category = (
                    "accepted"
                    if 200 <= response.status < 300
                    else "http_5xx" if response.status >= 500 else "http_4xx"
                )
            except CallbackError as exc:
                category = exc.reason
            self.store.record_attempt_receipt(
                self.owner_id, delivery.delivery_id, now=self.clock(), category=category
            )
            if response and 200 <= response.status < 300:
                self.store.ack(self.owner_id, delivery.delivery_id, delivery.claim_token)
                continue
            terminal = bool(
                response
                and (
                    response.status in (410, 413)
                    or (400 <= response.status < 500 and response.status not in (408, 429))
                )
            )
            delay = min(3600, 5 * 2 ** min(delivery.attempts - 1, 10))
            if response and response.headers.get("retry-after"):
                try:
                    delay = max(delay, float(response.headers["retry-after"]))
                except ValueError:
                    try:
                        delay = max(
                            delay,
                            (
                                parsedate_to_datetime(response.headers["retry-after"]) - now
                            ).total_seconds(),
                        )
                    except (ValueError, TypeError, OverflowError):
                        pass
            expiry = min(
                datetime.fromisoformat(grant["expires"]),
                datetime.fromisoformat(meta["created"]) + timedelta(hours=24),
            )
            due = now + timedelta(seconds=min(max(delay, 1), 86400))
            self.store.retry(
                self.owner_id,
                delivery.delivery_id,
                delivery.claim_token,
                due_at=min(due, expiry),
                dead=terminal or delivery.attempts >= 10 or due >= expiry,
            )
