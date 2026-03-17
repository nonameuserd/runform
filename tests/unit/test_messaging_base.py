from __future__ import annotations

from akc.ingest.connectors.messaging.base import Message, Thread, extract_qa_pairs


def test_extract_qa_pairs_skips_bots_and_empty_answers() -> None:
    root = Message(
        id="m1",
        channel_id="C1",
        user_id="U1",
        text="How do I deploy?",
        timestamp="1700000000.000001",
        thread_id="1700000000.000001",
        is_bot=False,
    )
    replies = (
        Message(
            id="m2",
            channel_id="C1",
            user_id="B1",
            text="Automated reply",
            timestamp="1700000001.000001",
            thread_id="1700000000.000001",
            is_bot=True,
        ),
        Message(
            id="m3",
            channel_id="C1",
            user_id="U2",
            text="",
            timestamp="1700000002.000001",
            thread_id="1700000000.000001",
            is_bot=False,
        ),
        Message(
            id="m4",
            channel_id="C1",
            user_id="U3",
            text="Use the staging pipeline first.",
            timestamp="1700000003.000001",
            thread_id="1700000000.000001",
            is_bot=False,
        ),
    )
    thread = Thread(channel_id="C1", thread_id="1700000000.000001", root=root, replies=replies)
    qa = extract_qa_pairs(thread, max_answers=3, include_bots=False)
    assert qa.question.id == "m1"
    assert [a.id for a in qa.answers] == ["m4"]
