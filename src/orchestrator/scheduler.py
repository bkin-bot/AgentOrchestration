"""Task Scheduler - Priority-based task queuing and dispatch."""
import heapq
import time
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4
class PriorityQueue:
    def __init__(self):
        self._queue = []
        self._counter = 0
    def push(self, item: Any, priority: int = 0) -> None:
        heapq.heappush(self._queue, (-priority, self._counter, item))
        self._counter += 1
    def pop(self) -> Optional[Any]:
        if self._queue:
            return heapq.heappop(self._queue)[2]
        return None
    def peek(self) -> Optional[Any]:
        if self._queue:
            return self._queue[0][2]
        return None
    def __len__(self) -> int:
        return len(self._queue)
class TaskScheduler:
    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self._clock = clock
        self._queues: Dict[str, PriorityQueue] = {}
        self._scheduled: Dict[str, Dict] = {}
        self._in_flight: Dict[str, Dict] = {}
        self._last_heartbeat: Dict[str, float] = {}
        self._audit: List[Dict] = []
        self._max_retries = 3
    def enqueue(self, task: Dict, queue: str = "default", priority: int = 0) -> str:
        task_id = str(uuid4())
        task["id"] = task_id
        task["enqueued_at"] = time.time()
        task["priority"] = priority
        task["retries"] = 0
        if queue not in self._queues:
            self._queues[queue] = PriorityQueue()
        self._queues[queue].push(task, priority)
        return task_id
    def schedule(self, task: Dict, delay: float, queue: str = "default", priority: int = 0) -> str:
        if delay < 0:
            raise ValueError("delay must be non-negative")
        task_id = str(uuid4())
        task["id"] = task_id
        task["retries"] = task.get("retries", 0)
        task["priority"] = priority
        self._scheduled[task_id] = {"task": task, "queue": queue, "priority": priority, "due_at": self._clock() + delay}
        return task_id
    async def dequeue(self, queue: str = "default", timeout: float = 1.0) -> Optional[Dict]:
        now = self._clock()
        expired = [tid for tid, scheduled in self._scheduled.items() if scheduled["queue"] == queue and scheduled["due_at"] <= now]
        for tid in expired:
            scheduled = self._scheduled.pop(tid)
            self._queues.setdefault(queue, PriorityQueue()).push(scheduled["task"], scheduled["priority"])
        if queue in self._queues and len(self._queues[queue]) > 0:
            task = self._queues[queue].pop()
            if task:
                self._in_flight[task["id"]] = task
                return task
        return None
    def complete(self, task_id: str) -> bool:
        completed = self._in_flight.pop(task_id, None) is not None
        if completed:
            self._last_heartbeat.pop(task_id, None)
        return completed
    def fail(self, task_id: str, queue: str = "default") -> bool:
        task = self._in_flight.pop(task_id, None)
        if task:
            self._last_heartbeat.pop(task_id, None)
            task["retries"] += 1
            if task["retries"] < self._max_retries:
                self.enqueue(task, queue, priority=task.get("priority", 0))
                return True
        return False
    def record_heartbeat(self, task_id: str, heartbeat_at: Optional[float] = None) -> bool:
        if task_id not in self._in_flight:
            self._audit_decision(task_id, "heartbeat_rejected", "task_not_in_flight")
            return False
        current = self._clock() if heartbeat_at is None else heartbeat_at
        previous = self._last_heartbeat.get(task_id)
        if previous is not None and current <= previous:
            self._audit_decision(task_id, "heartbeat_rejected", "non_monotonic_heartbeat")
            return False
        self._last_heartbeat[task_id] = current
        self._audit_decision(task_id, "heartbeat_accepted", "monotonic_heartbeat")
        return True
    def audit_log(self) -> List[Dict]:
        return list(self._audit)
    def _audit_decision(self, task_id: str, decision: str, reason: str) -> None:
        self._audit.append({"task_id": task_id, "decision": decision, "reason": reason, "recorded_at": self._clock()})
        self._audit = self._audit[-100:]
