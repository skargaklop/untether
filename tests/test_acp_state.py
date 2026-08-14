from untether.runners.acp.state import AcpSessionState


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


def test_reducer_bounds_unique_actions_and_unknown_updates() -> None:
    state = AcpSessionState(max_actions=3, max_unknown_updates=2)
    for index in range(20):
        state.apply(
            {
                "sessionUpdate": "tool_call",
                "toolCallId": f"tool-{index}",
                "title": "tool",
            }
        )
        state.apply({"sessionUpdate": "future_extension", "value": f"unknown-{index}"})

    assert len(state.actions) == 3
    assert set(state.actions) == {"tool:tool-17", "tool:tool-18", "tool:tool-19"}
    assert len(state.unknown_updates) == 2
    assert [item["value"] for item in state.unknown_updates] == [
        "unknown-18",
        "unknown-19",
    ]


def test_reducer_bounds_unique_terminal_projections() -> None:
    state = AcpSessionState(max_actions=3)
    for index in range(20):
        state.apply(
            {
                "sessionUpdate": "terminal_output",
                "terminalId": f"terminal-{index}",
                "data": "output",
            }
        )

    assert len(state.actions) == 3
    assert len(state._output) == 3
    assert set(state.actions) == {
        "terminal:terminal-17",
        "terminal:terminal-18",
        "terminal:terminal-19",
    }
