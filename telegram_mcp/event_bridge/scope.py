"""Default-deny exact scope matching and honest planned capability descriptions."""

from .models import Capability, ScopeTarget, WatchScope


class ScopeDenied(ValueError):
    pass


def matches_scope(scope: WatchScope, target: ScopeTarget) -> bool:
    """No aliases, wildcard, folder lookup or topic fallback is performed here."""
    return any(
        selected.account_id == target.account_id
        and selected.marked_chat_id == target.marked_chat_id
        and (selected.topic_id is None or selected.topic_id == target.topic_id)
        for selected in scope.targets
    )


def _planned(name, event_class, coverage, *delivery):
    return Capability("telegram." + name, event_class, False, coverage, tuple(delivery))


# Availability describes implemented handlers, not the intended release stage.
# No adapter is installed by this control-plane-only package.
CAPABILITY_CATALOG = (
    _planned("message.created", "message", "Observed updates in resolved selected chats", "batch"),
    _planned("message.edited", "message", "Observed edits only", "batch"),
    _planned("message.deleted", "message", "Requires reliable selected-chat attribution", "batch"),
    _planned("message.reactions_changed", "message", "Available reaction metadata only", "batch"),
    _planned(
        "chat.read_position_changed", "chat", "Read boundary, not individual readers", "silent"
    ),
    _planned("chat.unread_mark_changed", "chat", "Own unread mark only", "silent"),
    _planned("chat.message_pins_changed", "chat", "Pins in selected chat", "batch"),
    _planned("chat.dialog_pin_changed", "chat", "Own dialog organization", "silent"),
    _planned("chat.preferences_changed", "chat", "Available archive and mute changes", "silent"),
    _planned("topic.changed", "topic", "Selected topic only; loss never broadens scope", "batch"),
    _planned(
        "chat.membership_changed",
        "chat",
        "Observed selected-chat changes, not a full audit",
        "batch",
    ),
    _planned("chat.access_changed", "chat", "Available access-loss signals", "health"),
    _planned("chat.metadata_changed", "chat", "Available changed fields", "batch"),
    _planned("folder.definition_changed", "folder", "Selection review only", "silent"),
    _planned(
        "folder.membership_changed", "derived", "Computed snapshot difference only", "silent"
    ),
    _planned("draft.changed", "draft", "Own draft, not a send", "silent"),
    _planned("poll.changed", "poll", "Requires selected-message mapping", "batch"),
    _planned(
        "poll.vote_observed", "poll", "Only actually available non-anonymous identity", "batch"
    ),
    _planned("scheduled_message.changed", "message", "Scheduling is not actual sending", "silent"),
    _planned(
        "message.metrics_changed", "message", "Available selected-publication counters", "batch"
    ),
    _planned("call.state_changed", "call", "Attributed metadata only; no audio", "batch"),
    _planned("presence.changed", "ephemeral", "Excluded; no retention or replay"),
    _planned("typing.changed", "ephemeral", "Excluded; no retention or replay"),
    _planned("stories", "reserved", "Account and API dependent; excluded"),
    _planned("business", "reserved", "Account and API dependent; excluded"),
    _planned("quick_replies", "reserved", "Account and API dependent; excluded"),
    _planned("monetization", "reserved", "Account and API dependent; excluded"),
    _planned("conversation.settled", "derived", "Settled selected-conversation batch", "internal"),
    _planned("obligation.changed", "derived", "Requires scoped analysis and evidence", "internal"),
    _planned(
        "reply.candidate_observed",
        "derived",
        "Quiet evidence; does not close obligations",
        "silent",
    ),
    _planned(
        "obligation.due", "derived", "Requires separately confirmed reminder rule", "attention"
    ),
    _planned("digest.ready", "derived", "Requires configured schedule or quiet hours", "batch"),
    _planned("processing.completed", "derived", "Previously authorized media task only", "batch"),
    _planned(
        "activity.batch_ready", "delivery", "Selected changes, outgoing-only silent", "batch"
    ),
    _planned("attention.due", "delivery", "Recheck current state before delivery", "attention"),
    _planned("watch.health_changed", "operational", "Coalesced health; no secrets", "health"),
)
