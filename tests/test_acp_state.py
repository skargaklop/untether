import pytest

from untether.runners.acp.peer import AcpProtocolError
from untether.runners.acp.state import AcpMessageLedger, AcpSessionState


def test_message_ledger_bounds_repeated_chunks_and_replacements() -> None:
    ledger = AcpMessageLedger(max_answer=5, max_message_content=4, max_messages=2)
    for chunk in ("abcdef", "ghijkl", "mnopqr"):
        ledger.update("m1", "assistant", chunk)

    assert ledger.answer == "nopqr"
    assert ledger.answer_parts["m1"] == "nopqr"
    assert ledger.messages["m1"]["content"] == "opqr"

    ledger.update("m1", "assistant", "replacement", replace=True)
    assert ledger.answer == "ement"
    assert ledger.answer_parts["m1"] == "ement"
    assert ledger.messages["m1"]["content"] == "ment"


def test_session_reducer_uses_bounded_message_ledger_for_repeated_chunks() -> None:
    state = AcpSessionState(max_answer=5, max_message_content=4)
    for _ in range(1000):
        state.apply(
            {
                "sessionUpdate": "message",
                "messageId": "m1",
                "content": "chunk",
            }
        )

    assert not hasattr(state, "_answer_messages")
    assert len(state._message_ledger.answer_parts["m1"]) <= 5
    assert len(state.messages["m1"]["content"]) <= 4
    assert len(state.answer) <= 5


def test_message_replacements_and_metadata_are_reduced_without_corrupting_text():
    state = AcpSessionState()
    state.apply({"sessionUpdate": "message", "messageId": "m1", "content": "old"})
    state.apply(
        {
            "sessionUpdate": "message",
            "messageId": "m1",
            "content": "new",
            "replace": True,
        }
    )
    state.apply({"sessionUpdate": "metadata", "metadata": {"branch": "main"}})
    assert state.answer == "new"
    assert state.messages["m1"]["content"] == "new"
    assert state.metadata == {"branch": "main"}


def test_terminal_plain_text_is_not_guessed_as_base64_and_unknown_update_is_retained():
    state = AcpSessionState()
    state.apply(
        {"sessionUpdate": "terminal_output", "terminalId": "t1", "data": "test"}
    )
    state.apply({"sessionUpdate": "future_update", "value": 1})
    assert state._output["t1"] == "test"
    assert state.unknown_updates[-1]["sessionUpdate"] == "future_update"


def test_reducer_keeps_stable_ids_and_updates_message_tool_plan_terminal_diff_usage() -> (
    None
):
    state = AcpSessionState()
    assert (
        state.apply(
            {"sessionUpdate": "agent_message_chunk", "content": {"text": "Hello"}}
        )
        == []
    )
    events = state.apply(
        {"sessionUpdate": "tool_call", "toolCallId": "t1", "title": "Read"}
    )
    assert events[0].action.id == "tool:t1"
    update = state.apply(
        {
            "sessionUpdate": "tool_call_update",
            "toolCallId": "t1",
            "status": "completed",
            "output": "ok",
        }
    )
    assert update[0].action.id == "tool:t1"
    assert update[0].phase == "completed"
    plan = state.apply(
        {
            "sessionUpdate": "plan",
            "planId": "p1",
            "entries": [{"content": "test", "status": "pending"}],
        }
    )
    assert plan[0].action.id == "plan:p1"
    terminal = state.apply(
        {"sessionUpdate": "terminal_output", "terminalId": "term", "data": "out"}
    )
    assert terminal[0].action.id == "terminal:term"
    diff = state.apply(
        {
            "sessionUpdate": "tool_call_update",
            "toolCallId": "t1",
            "diff": {"path": "a.py", "oldText": "x", "newText": "y"},
        }
    )
    assert diff[0].action.kind == "file_change"
    state.apply(
        {
            "sessionUpdate": "usage_update",
            "usage": {"inputTokens": 2, "outputTokens": 3},
            "cost": 0.4,
        }
    )
    assert state.usage["total_cost_usd"] == 0.4
    assert state.answer == "Hello"


def test_reducer_handles_v1_names_and_unknown_variants_without_crashing() -> None:
    state = AcpSessionState()
    state.apply({"type": "message", "role": "assistant", "content": "one"})
    state.apply({"type": "message", "role": "assistant", "content": {"text": " two"}})
    unknown = state.apply({"sessionUpdate": "future_extension", "value": "safe"})
    assert unknown == []
    assert state.unknown_updates[-1]["sessionUpdate"] == "future_extension"
    assert state.answer == "one two"


def test_replayed_assistant_updates_are_not_appended_to_current_answer() -> None:
    state = AcpSessionState()
    state.apply({"sessionUpdate": "agent_message_chunk", "content": "old"})
    state.begin_prompt(state.answer)
    state.apply({"sessionUpdate": "agent_message_chunk", "content": "old"})
    state.apply({"sessionUpdate": "agent_message_chunk", "content": "new"})
    assert state.answer == "new"


def test_replayed_assistant_updates_still_rebuild_message_projection() -> None:
    state = AcpSessionState()
    state.apply({"sessionUpdate": "message", "messageId": "m1", "content": "old"})
    state.begin_prompt(state.answer)
    state.apply({"sessionUpdate": "message", "messageId": "m1", "content": "old"})

    assert state.messages["m1"]["content"] == "oldold"
    assert state.answer == ""


def test_terminal_base64_chunks_are_decoded_and_bounded() -> None:
    state = AcpSessionState(max_output=4)
    state.apply(
        {
            "sessionUpdate": "terminal_output",
            "terminalId": "x",
            "data": "YWJj",
            "encoding": "base64",
        }
    )
    state.apply(
        {
            "sessionUpdate": "terminal_output",
            "terminalId": "x",
            "data": "ZA==",
            "encoding": "base64",
        }
    )
    assert state.actions["terminal:x"].detail["output"] == "abcd"


def test_reducer_bounds_answer_and_message_content() -> None:
    state = AcpSessionState(max_answer=5, max_message_content=4)
    for index in range(20):
        state.apply(
            {
                "sessionUpdate": "message",
                "messageId": f"message-{index}",
                "content": "abcdefgh",
            }
        )

    assert len(state.answer) <= 5
    assert all(len(message["content"]) <= 4 for message in state.messages.values())


def test_replacing_truncated_assistant_message_replaces_answer_snapshot() -> None:
    state = AcpSessionState(max_answer=5)
    state.apply({"sessionUpdate": "message", "messageId": "m1", "content": "abcdef"})
    state.apply(
        {
            "sessionUpdate": "message",
            "messageId": "m1",
            "content": "new",
            "replace": True,
        }
    )

    assert state.answer == "new"
    assert state.messages["m1"]["content"] == "new"


_PROTOCOL_KEYS = ["sessionUpdate", "type"]


@pytest.mark.parametrize("key", _PROTOCOL_KEYS)
def test_agent_message_upsert_replaces_on_any_protocol_key(key) -> None:
    state = AcpSessionState()
    state.apply({key: "agent_message", "messageId": "m1", "content": "first"})
    state.apply({key: "agent_message", "messageId": "m1", "content": "second"})
    assert state.messages["m1"]["content"] == "second"
    assert state.answer == "second"


def test_agent_message_with_content_blocks_joins_into_answer() -> None:
    state = AcpSessionState()
    state.apply(
        {
            "sessionUpdate": "agent_message",
            "messageId": "m1",
            "content": [
                {"type": "text", "text": "Hello"},
                {"type": "text", "text": " world"},
            ],
        }
    )
    assert state.answer == "Hello world"


@pytest.mark.parametrize("key", _PROTOCOL_KEYS)
def test_agent_message_chunk_appends_on_any_protocol_key(key) -> None:
    state = AcpSessionState()
    state.apply({key: "agent_message_chunk", "messageId": "m1", "content": "hel"})
    state.apply({key: "agent_message_chunk", "messageId": "m1", "content": "lo"})
    assert state.messages["m1"]["content"] == "hello"
    assert state.answer == "hello"


@pytest.mark.parametrize("kind", ["agent_thought", "agent_thought_chunk"])
@pytest.mark.parametrize("key", _PROTOCOL_KEYS)
def test_agent_thought_variants_emit_note_action(key, kind) -> None:
    state = AcpSessionState()
    events = state.apply({key: kind, "messageId": "t1", "content": "thinking"})
    assert events[0].action.id == "thought:t1"
    assert events[0].action.kind == "note"
    assert events[0].action.title == "thinking"


def test_agent_thought_chunk_appends_accumulated_text() -> None:
    state = AcpSessionState()
    state.apply(
        {"sessionUpdate": "agent_thought_chunk", "messageId": "t1", "content": "rea"}
    )
    events = state.apply(
        {"sessionUpdate": "agent_thought_chunk", "messageId": "t1", "content": "soning"}
    )
    assert events[0].action.title == "reasoning"


@pytest.mark.parametrize("kind", ["user_message", "user_message_chunk"])
@pytest.mark.parametrize("key", _PROTOCOL_KEYS)
def test_user_message_variants_track_ledger_not_answer(key, kind) -> None:
    state = AcpSessionState()
    state.apply({key: kind, "messageId": "u1", "content": "user says"})
    assert state.messages["u1"]["content"] == "user says"
    assert state.answer == ""


@pytest.mark.parametrize("key", _PROTOCOL_KEYS)
def test_tool_call_content_chunk_appends_content(key) -> None:
    state = AcpSessionState()
    state.apply({key: "tool_call", "toolCallId": "t1", "title": "Run"})
    first = state.apply(
        {key: "tool_call_content_chunk", "toolCallId": "t1", "content": "abc"}
    )
    assert first[0].action.detail["content"] == "abc"
    second = state.apply(
        {key: "tool_call_content_chunk", "toolCallId": "t1", "content": "def"}
    )
    assert second[0].action.detail["content"] == "abcdef"


@pytest.mark.parametrize("key", _PROTOCOL_KEYS)
def test_terminal_update_tracks_status(key) -> None:
    state = AcpSessionState()
    started = state.apply(
        {key: "terminal_update", "terminalId": "x", "status": "created"}
    )
    assert started[0].action.id == "terminal:x"
    assert started[0].phase == "started"
    exited = state.apply(
        {key: "terminal_update", "terminalId": "x", "status": "exited", "exitStatus": 0}
    )
    assert exited[0].phase == "completed"
    assert exited[0].action.detail.get("exitStatus") == 0


@pytest.mark.parametrize("key", _PROTOCOL_KEYS)
def test_terminal_output_chunk_reuses_base64_path(key) -> None:
    state = AcpSessionState(max_output=4)
    state.apply(
        {
            key: "terminal_output_chunk",
            "terminalId": "x",
            "data": "YWJj",
            "encoding": "base64",
        }
    )
    state.apply(
        {
            key: "terminal_output_chunk",
            "terminalId": "x",
            "data": "ZA==",
            "encoding": "base64",
        }
    )
    assert state.actions["terminal:x"].detail["output"] == "abcd"


@pytest.mark.parametrize("key", _PROTOCOL_KEYS)
def test_session_info_update_merges_metadata(key) -> None:
    state = AcpSessionState()
    state.apply(
        {
            key: "session_info_update",
            "title": "My Session",
            "metadata": {"branch": "main"},
        }
    )
    assert state.metadata["branch"] == "main"
    assert state.metadata.get("title") == "My Session"


@pytest.mark.parametrize("key", _PROTOCOL_KEYS)
def test_available_commands_update_tracks_names_verbatim(key) -> None:
    state = AcpSessionState()
    state.apply(
        {
            key: "available_commands_update",
            "availableCommands": [{"name": "message:ls"}, {"name": "status"}],
        }
    )
    state.apply(
        {
            key: "available_commands_update",
            "availableCommands": [{"name": "message:ls"}, {"name": "help"}],
        }
    )
    assert state.available_commands == {"message:ls", "status", "help"}


@pytest.mark.parametrize("key", _PROTOCOL_KEYS)
def test_config_option_update_tracks_options(key) -> None:
    state = AcpSessionState()
    state.apply(
        {
            key: "config_option_update",
            "configId": "mode",
            "category": "thought_level",
            "value": "high",
        }
    )
    assert any(item.get("configId") == "mode" for item in state.config_options)


@pytest.mark.parametrize("kind", ["mode_update", "current_mode_update"])
@pytest.mark.parametrize("key", _PROTOCOL_KEYS)
def test_mode_update_variants_track_mode(key, kind) -> None:
    state = AcpSessionState()
    state.apply({key: kind, "mode": "architect"})
    assert state.mode == "architect"


def test_agent_message_patch_omitted_null_value() -> None:
    state = AcpSessionState()
    state.apply(
        {"sessionUpdate": "agent_message", "messageId": "m1", "content": "base"}
    )
    state.apply({"sessionUpdate": "agent_message", "messageId": "m1"})
    assert state.messages["m1"]["content"] == "base"
    state.apply({"sessionUpdate": "agent_message", "messageId": "m1", "content": None})
    assert state.messages["m1"]["content"] == ""
    assert state.answer == ""
    state.apply(
        {"sessionUpdate": "agent_message", "messageId": "m1", "content": "final"}
    )
    assert state.messages["m1"]["content"] == "final"
    assert state.answer == "final"


def test_tool_call_update_patch_omitted_null_value() -> None:
    state = AcpSessionState()
    state.apply(
        {
            "sessionUpdate": "tool_call",
            "toolCallId": "t1",
            "title": "A",
            "status": "started",
        }
    )
    assert state.actions["tool:t1"].title == "A"
    state.apply({"sessionUpdate": "tool_call_update", "toolCallId": "t1"})
    assert state.actions["tool:t1"].title == "A"
    state.apply(
        {"sessionUpdate": "tool_call_update", "toolCallId": "t1", "title": None}
    )
    assert state.actions["tool:t1"].title == "t1"
    completed = state.apply(
        {"sessionUpdate": "tool_call_update", "toolCallId": "t1", "status": "completed"}
    )
    assert completed[0].phase == "completed"


@pytest.mark.parametrize(
    "update",
    [
        {"sessionUpdate": "tool_call_update"},
        {"sessionUpdate": "tool_call", "title": "x"},
        {"sessionUpdate": "tool_call_content_chunk", "content": "x"},
        {"sessionUpdate": "terminal_update"},
        {"sessionUpdate": "terminal_output_chunk", "data": "x"},
    ],
)
def test_malformed_known_kind_raises_protocol_error(update) -> None:
    state = AcpSessionState()
    with pytest.raises(AcpProtocolError):
        state.apply(update)


def test_message_aggregate_overflow_fails() -> None:
    state = AcpSessionState()
    for index in range(256):
        state.apply(
            {"sessionUpdate": "message", "messageId": f"m{index}", "content": "x"}
        )
    with pytest.raises(AcpProtocolError, match="aggregate overflow: messages"):
        state.apply({"sessionUpdate": "message", "messageId": "m256", "content": "x"})


def test_actions_aggregate_overflow_fails() -> None:
    state = AcpSessionState(max_actions=3)
    for index in range(3):
        state.apply(
            {"sessionUpdate": "tool_call", "toolCallId": f"t{index}", "title": "x"}
        )
    with pytest.raises(AcpProtocolError, match="aggregate overflow: actions"):
        state.apply({"sessionUpdate": "tool_call", "toolCallId": "t3", "title": "x"})


def test_unknown_updates_aggregate_overflow_fails() -> None:
    state = AcpSessionState(max_unknown_updates=2)
    state.apply({"sessionUpdate": "future_1"})
    state.apply({"sessionUpdate": "future_2"})
    with pytest.raises(AcpProtocolError, match="aggregate overflow: unknown_updates"):
        state.apply({"sessionUpdate": "future_3"})


def test_large_single_message_still_succeeds() -> None:
    state = AcpSessionState()
    big = "x" * 64000
    state.apply({"sessionUpdate": "message", "messageId": "m1", "content": big})
    assert len(state.messages["m1"]["content"]) == 64000


def test_tracking_fields_persist_across_begin_prompt() -> None:
    state = AcpSessionState()
    state.apply(
        {
            "sessionUpdate": "available_commands_update",
            "availableCommands": [{"name": "message:ls"}],
        }
    )
    state.apply(
        {"sessionUpdate": "config_option_update", "configId": "mode", "value": "plan"}
    )
    state.apply({"sessionUpdate": "mode_update", "mode": "plan"})
    state.begin_prompt()
    assert state.available_commands == {"message:ls"}
    assert state.config_options != []
    assert state.mode == "plan"
