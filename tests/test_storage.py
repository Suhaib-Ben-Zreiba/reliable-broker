"""Unit tests for the persistent log, isolated from TopicRegistry and from
the network layer -- same isolation approach as protocol.py and topics.py."""

from broker.storage import LogStore


def test_read_all_on_empty_topic_returns_empty_list(tmp_path):
    store = LogStore(tmp_path)

    assert store.read_all("orders") == []


def test_append_then_read_all_round_trips(tmp_path):
    store = LogStore(tmp_path)

    store.append("orders", {"message_id": "m-1", "body": {"id": 1}})
    store.append("orders", {"message_id": "m-2", "body": {"id": 2}})

    records = store.read_all("orders")
    assert [r["message_id"] for r in records] == ["m-1", "m-2"]


def test_topics_are_stored_independently(tmp_path):
    store = LogStore(tmp_path)

    store.append("orders", {"message_id": "m-1", "body": {"id": 1}})
    store.append("shipping", {"message_id": "m-2", "body": {"id": 2}})

    assert len(store.read_all("orders")) == 1
    assert len(store.read_all("shipping")) == 1


def test_data_survives_across_separate_store_instances(tmp_path):
    """Simulates a broker restart: a fresh LogStore pointed at the same
    directory must see what a previous instance wrote."""
    first = LogStore(tmp_path)
    first.append("orders", {"message_id": "m-1", "body": {"id": 1}})

    second = LogStore(tmp_path)
    assert second.read_all("orders") == [{"message_id": "m-1", "body": {"id": 1}}]


def test_max_message_id_seen_is_zero_for_empty_store(tmp_path):
    store = LogStore(tmp_path)

    assert store.max_message_id_seen() == 0


def test_max_message_id_seen_finds_highest_across_topics(tmp_path):
    store = LogStore(tmp_path)
    store.append("orders", {"message_id": "m-3", "body": {}})
    store.append("shipping", {"message_id": "m-7", "body": {}})
    store.append("orders", {"message_id": "m-5", "body": {}})

    assert store.max_message_id_seen() == 7


def test_topic_name_cannot_escape_the_data_directory(tmp_path):
    store = LogStore(tmp_path)

    store.append("../../etc/passwd", {"message_id": "m-1", "body": {}})

    # The write must have landed inside tmp_path, not outside it.
    written_files = list(tmp_path.glob("*.jsonl"))
    assert len(written_files) == 1
    assert written_files[0].parent == tmp_path
