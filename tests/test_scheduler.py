import asyncio
from src.orchestrator.scheduler import TaskScheduler
class FakeClock:
    def __init__(self, now=0.0):
        self.now = now
    def __call__(self):
        return self.now
    def advance(self, seconds):
        self.now += seconds
class TestTaskScheduler:
    def setup_method(self):
        self.scheduler = TaskScheduler()
    def test_enqueue_task(self):
        task_id = self.scheduler.enqueue({"type": "test", "payload": {}})
        assert task_id is not None
    def test_dequeue_task(self):
        self.scheduler.enqueue({"type": "test", "payload": {"data": 1}})
        task = asyncio.run(self.scheduler.dequeue())
        assert task is not None
        assert task["type"] == "test"
    def test_enqueue_multiple_priorities(self):
        self.scheduler.enqueue({"type": "low"}, priority=1)
        self.scheduler.enqueue({"type": "high"}, priority=10)
        task = asyncio.run(self.scheduler.dequeue())
        assert task["type"] == "high"
    def test_complete_task(self):
        self.scheduler.enqueue({"type": "test"})
        task = asyncio.run(self.scheduler.dequeue())
        assert self.scheduler.complete(task["id"])
    def test_fail_task_with_retry(self):
        self.scheduler.enqueue({"type": "test"})
        task = asyncio.run(self.scheduler.dequeue())
        assert self.scheduler.fail(task["id"])
    def test_scheduled_tasks_use_monotonic_clock(self):
        clock = FakeClock(100.0)
        scheduler = TaskScheduler(clock=clock)
        scheduler.schedule({"type": "scheduled"}, delay=5.0)
        assert asyncio.run(scheduler.dequeue()) is None
        clock.advance(5.0)
        task = asyncio.run(scheduler.dequeue())
        assert task is not None
        assert task["type"] == "scheduled"
    def test_rejects_non_monotonic_heartbeat(self):
        clock = FakeClock(100.0)
        scheduler = TaskScheduler(clock=clock)
        scheduler.enqueue({"type": "heartbeat"})
        task = asyncio.run(scheduler.dequeue())
        assert scheduler.record_heartbeat(task["id"])
        clock.advance(-10.0)
        assert not scheduler.record_heartbeat(task["id"])
        assert task["id"] in scheduler._in_flight
        assert scheduler.audit_log()[-1]["reason"] == "non_monotonic_heartbeat"
