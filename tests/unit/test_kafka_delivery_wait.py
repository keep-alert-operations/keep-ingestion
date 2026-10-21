"""Delivery reports from one poll thread resolve per-message futures on the waiting loop."""

import asyncio
import threading
from time import monotonic, sleep
from types import SimpleNamespace
from uuid import uuid4

import pytest

from appchi.ingestion import kafka_admission
from appchi.ingestion.kafka_admission import KafkaAdmission, KafkaPublisher
from appchi.ingestion.native import canonical


class FakeProducer:
    """librdkafka-like: callbacks run only inside poll()/flush() on the calling thread."""

    def __init__(self, config):
        self.config = config
        self.condition = threading.Condition()
        self.queue = []  # [due, on_delivery, error]
        self.poll_calls = []
        self.poll_threads = set()
        self.flushed = []
        self.callback_threads = set()
        self.produce_error = None
        self.next_error = None
        self.next_delay = 0.0
        self.hold = False  # never acknowledge

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self.produce_error:
            raise self.produce_error
        with self.condition:
            due = float("inf") if self.hold else monotonic() + self.next_delay
            self.queue.append([due, on_delivery, self.next_error])
            self.condition.notify_all()

    def _serve(self, deadline):
        with self.condition:
            while True:
                now = monotonic()
                ready = [item for item in self.queue if item[0] <= now]
                if ready or now >= deadline:
                    break
                upcoming = min((item[0] for item in self.queue), default=deadline)
                self.condition.wait(max(0, min(deadline, upcoming) - now))
            for item in ready:
                self.queue.remove(item)
        for _, callback, error in ready:
            self.callback_threads.add(threading.get_ident())
            callback(error, None)
        return len(ready)

    def poll(self, timeout=None):
        self.poll_calls.append(timeout)
        self.poll_threads.add(threading.get_ident())
        return self._serve(monotonic() + (timeout or 0))

    def flush(self, timeout=None):
        self.flushed.append(timeout)
        self._serve(monotonic())
        with self.condition:
            return len(self.queue)


@pytest.fixture
def fake(monkeypatch):
    import confluent_kafka
    import appchi.service_clients

    made = []

    def factory(config):
        made.append(FakeProducer(config))
        return made[-1]

    monkeypatch.setattr(confluent_kafka, "Producer", factory)
    monkeypatch.setattr(appchi.service_clients, "kafka_options", lambda settings: {})
    publisher = KafkaPublisher(SimpleNamespace(kafka_bootstrap="fake:9092"))
    yield publisher, made[0]
    publisher.close()


def test_producer_settings_are_unchanged(fake):
    _, producer = fake
    assert producer.config == {
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "delivery.timeout.ms": 1000,
        "request.timeout.ms": 1000,
        "linger.ms": 0,
    }
    assert kafka_admission.ATTEMPT_SECONDS == 1.1


def test_ack_resolves_from_poll_thread_without_busy_polling(fake):
    publisher, producer = fake
    producer.next_delay = 0.05

    async def run():
        started = monotonic()
        await publisher.publish("t", b"k", b"v")
        return monotonic() - started

    elapsed = asyncio.run(run())
    assert 0.045 <= elapsed < 0.09
    # Only the background thread polls, and it blocks rather than spinning on poll(0).
    assert producer.poll_threads and threading.get_ident() not in producer.poll_threads
    assert producer.callback_threads <= producer.poll_threads
    assert set(producer.poll_calls) == {kafka_admission.POLL_SECONDS}
    assert len(producer.poll_calls) < 10


def test_full_batch_waits_about_as_long_as_slowest_ack(fake, v1):
    publisher, producer = fake
    slow = [0.002 + 0.001 * i for i in range(100)]  # slowest ack ~101 ms
    delays = [0.0] * 100
    original = producer.produce
    order = []

    def produce(topic, key=None, value=None, on_delivery=None):
        producer.next_delay = delays[len(order) % 100]
        order.append(value)
        original(topic, key=key, value=value, on_delivery=on_delivery)

    producer.produce = produce
    members = [dict(v1, application=f"batch-{i}") for i in range(100)]
    raw = canonical({"alerts": [{"payload": m} for m in members]})
    admission = KafkaAdmission(publisher, environment="development", global_tenant_id=uuid4())

    def run():
        order.clear()
        started = monotonic()
        result = asyncio.run(admission.admit(raw, "v1"))
        return result, monotonic() - started

    run()  # warm imports and validation caches
    _, overhead = min((run() for _ in range(3)), key=lambda r: r[1])  # immediate acks
    delays[:] = slow
    polls = len(producer.poll_calls)
    (status, body), elapsed = run()
    assert status == 202
    assert [r["index"] for r in body["results"]] == list(range(100))
    assert {r["status"] for r in body["results"]} == {202}
    assert len(order) == 100
    # The batch costs its own processing plus the slowest acknowledgement, not a sum
    # of per-member polling delays.
    assert max(slow) <= elapsed < overhead + max(slow) + 0.04
    assert len(producer.poll_calls) - polls < 20  # not one poll loop per member


def test_delivery_error_maps_to_unconfirmed_after_three_attempts(fake, v1):
    publisher, producer = fake
    producer.next_error = RuntimeError("broker rejected")

    with pytest.raises(RuntimeError, match="kafka_publish_unconfirmed"):
        asyncio.run(publisher.publish("t", b"k", b"v"))

    admission = KafkaAdmission(publisher, environment="development", global_tenant_id=uuid4())
    status, body = asyncio.run(admission.admit(canonical(v1), "v1"))
    assert status == 503
    assert body["results"][0]["error"] == {
        "code": "kafka_publish_unconfirmed",
        "message": "Kafka publication was not confirmed after three attempts.",
        "retryable": True,
    }


def test_synchronous_produce_error_still_propagates(fake, v1):
    publisher, producer = fake
    producer.produce_error = BufferError("queue full")
    with pytest.raises(BufferError):
        asyncio.run(publisher.publish("t", b"k", b"v"))
    assert not publisher._pending
    status, body = asyncio.run(
        KafkaAdmission(publisher, environment="development", global_tenant_id=uuid4()).admit(
            canonical(v1), "v1"
        )
    )
    assert status == 503 and body["results"][0]["disposition"] == "publish_failed"


def test_mixed_batch_keeps_per_member_failures(fake, v1):
    publisher, producer = fake
    original = producer.produce

    def produce(topic, key=None, value=None, on_delivery=None):
        producer.next_error = RuntimeError("fail") if b"bad-app" in value else None
        original(topic, key=key, value=value, on_delivery=on_delivery)

    producer.produce = produce
    batch = {"alerts": [{"payload": dict(v1, application="good-app")},
                        {"payload": dict(v1, application="bad-app")}]}
    status, body = asyncio.run(
        KafkaAdmission(publisher, environment="development", global_tenant_id=uuid4()).admit(
            canonical(batch), "v1"
        )
    )
    assert status == 207
    assert [r["status"] for r in body["results"]] == [202, 503]


def test_unacknowledged_attempt_times_out_at_the_same_bound(fake):
    publisher, producer = fake
    producer.hold = True

    async def run():
        started = monotonic()
        with pytest.raises(RuntimeError, match="kafka_publish_unconfirmed"):
            await publisher.publish("t", b"k", b"v")
        return monotonic() - started

    elapsed = asyncio.run(run())
    assert 1.1 <= elapsed < 1.3
    assert not publisher._pending


def test_cancelled_wait_does_not_break_the_poll_thread(fake):
    publisher, producer = fake
    producer.next_delay = 0.1

    async def run():
        task = asyncio.create_task(publisher.publish("t", b"k", b"cancelled"))
        await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0.15)  # the cancelled record's report arrives now
        producer.next_delay = 0.01
        await publisher.publish("t", b"k", b"next")

    asyncio.run(run())
    assert publisher._poller.is_alive()
    assert not publisher._pending


def test_report_after_waiting_loop_closed_keeps_poll_thread_alive(fake):
    publisher, producer = fake
    producer.next_delay = 0.2

    async def abandon():
        task = asyncio.create_task(publisher.publish("t", b"k", b"v"))
        await asyncio.sleep(0.01)
        task.cancel()

    asyncio.run(abandon())  # loop is closed before the report arrives
    sleep(0.3)
    assert publisher._poller.is_alive()
    producer.next_delay = 0
    asyncio.run(publisher.publish("t", b"k", b"v"))


def test_concurrent_event_loops_share_one_publisher(fake):
    publisher, producer = fake
    producer.next_delay = 0.02
    failures = []

    def worker():
        async def run():
            await asyncio.gather(*(publisher.publish("t", b"k", b"v") for _ in range(20)))

        try:
            asyncio.run(run())
        except Exception as error:  # pragma: no cover - reported below
            failures.append(error)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)
    assert not failures and not any(t.is_alive() for t in threads)


def test_close_stops_poller_flushes_and_fails_pending_futures(fake):
    publisher, producer = fake
    producer.hold = True

    async def run():
        task = asyncio.create_task(publisher.publish("t", b"k", b"v"))
        await asyncio.sleep(0.02)
        started = monotonic()
        await asyncio.to_thread(publisher.close)
        with pytest.raises(RuntimeError, match="kafka_publish_unconfirmed"):
            await task
        return monotonic() - started

    elapsed = asyncio.run(run())
    assert elapsed < 0.5  # failed by shutdown, not by the 1.1 s attempt bound
    assert not publisher._poller.is_alive()
    assert producer.flushed == [1.1]
    assert not publisher._pending
    with pytest.raises(RuntimeError, match="kafka_publish_unconfirmed"):
        asyncio.run(publisher.publish("t", b"k", b"after-close"))


def test_close_delivers_reports_served_by_flush(fake):
    publisher, producer = fake
    producer.next_delay = 0.0

    async def run():
        # Stop the poller first so only flush can serve this report.
        publisher._stopping.set()
        publisher._poller.join()
        task = asyncio.create_task(publisher.publish("t", b"k", b"v"))
        await asyncio.sleep(0.01)
        await asyncio.to_thread(publisher.close)
        await task

    asyncio.run(run())
