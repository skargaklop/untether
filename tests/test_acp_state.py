from untether.runners.acp.state import AcpSessionState


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
    assert update[0].phase == "updated"
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
    assert unknown and unknown[0].action.kind == "note"
    assert state.answer == "one two"


def test_terminal_base64_chunks_are_decoded_and_bounded() -> None:
    state = AcpSessionState(max_output=4)
    state.apply({"sessionUpdate": "terminal_output", "terminalId": "x", "data": "YWJj"})
    state.apply({"sessionUpdate": "terminal_output", "terminalId": "x", "data": "ZA=="})
    assert state.actions["terminal:x"].detail["output"] == "abcd"
