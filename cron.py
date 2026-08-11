import asyncio
from dataclasses import dataclass, field
from datetime import datetime
import logging
from typing import Any, Dict, List, Optional

# Setup standard logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("TaskScheduler")


# ==========================================
# 1. Action Strategies (OOP Design)
# ==========================================
class BaseStrategy:
    """Interface for dynamic task execution strategies."""

    async def execute(self, target: str, params: Dict[str, Any]) -> bool:
        raise NotImplementedError


class SyncStrategy(BaseStrategy):

    async def execute(self, target: str, params: Dict[str, Any]) -> bool:
        depth = params.get("depth", "full")
        logger.info(
            f"[SYNC] Synchronizing target: {target} (Depth: {depth})..."
        )
        return True


class BackupStrategy(BaseStrategy):

    async def execute(self, target: str, params: Dict[str, Any]) -> bool:
        compress = params.get("compress", False)
        logger.info(
            f"[BACKUP] Backing up target: {target} (Compression: {compress})..."
        )
        return True


class DeleteStrategy(BaseStrategy):

    async def execute(self, target: str, params: Dict[str, Any]) -> bool:
        force = params.get("force", False)
        logger.info(f"[DELETE] Purging target: {target} (Force: {force})...")
        return True


class DefaultStrategy(BaseStrategy):

    async def execute(self, target: str, params: Dict[str, Any]) -> bool:
        logger.info(f"[DEFAULT] Running action on target: {target}")
        return True


# ==========================================
# 2. Data Models
# ==========================================
@dataclass
class User:
    username: str
    quota: int
    executed: int = 0

    def has_quota(self) -> bool:
        return self.executed < self.quota

    def consume_quota(self) -> None:
        self.executed += 1


@dataclass
class Task:
    task_id: str
    username: str
    scheduled_time: str  # Format "HH:MM"
    action: str
    target: str
    params: Dict[str, Any] = field(default_factory=dict)


# ==========================================
# 3. User & Quota Management Module
# ==========================================
class UserManager:

    def __init__(self):
        self._users: Dict[str, User] = {}

    def add_user(self, username: str, quota: int) -> None:
        self._users[username] = User(username=username, quota=quota)

    def get_user(self, username: str) -> Optional[User]:
        return self._users.get(username)

    def check_and_consume_quota(self, username: str) -> bool:
        user = self.get_user(username)
        if not user:
            logger.error(f"Execution failed: User '{username}' does not exist.")
            return False

        if not user.has_quota():
            logger.warning(
                f"Execution denied: User '{username}' exceeded quota ({user.executed}/{user.quota})."
            )
            return False

        user.consume_quota()
        return True


# ==========================================
# 4. Extensible Task Executor
# ==========================================
class TaskExecutor:

    def __init__(self):
        self._strategies: Dict[str, BaseStrategy] = {
            "sync": SyncStrategy(),
            "backup": BackupStrategy(),
            "delete": DeleteStrategy(),
        }
        self._default_strategy = DefaultStrategy()

    def register_strategy(self, action: str, strategy: BaseStrategy) -> None:
        """Dynamically add new action strategies."""
        self._strategies[action] = strategy

    async def run_task(self, task: Task) -> bool:
        strategy = self._strategies.get(task.action, self._default_strategy)
        try:
            return await strategy.execute(task.target, task.params)
        except Exception as e:
            logger.error(
                f"Error executing task '{task.task_id}' ({task.action}): {e}"
            )
            return False


# ==========================================
# 5. Scheduling System (Async Engine)
# ==========================================
class SchedulerEngine:

    def __init__(self, user_manager: UserManager, executor: TaskExecutor):
        self.user_manager = user_manager
        self.executor = executor
        self.tasks: List[Task] = []

    def add_task(self, task: Task) -> None:
        self.tasks.append(task)

    async def execute_due_tasks(
        self, current_time: Optional[str] = None
    ) -> None:
        now = current_time or datetime.now().strftime("%H:%M")
        due_tasks = [t for t in self.tasks if t.scheduled_time == now]

        if not due_tasks:
            logger.info(f"No tasks scheduled for {now}.")
            return

        logger.info(
            f"--- Processing {len(due_tasks)} scheduled task(s) at {now} ---"
        )

        # Process due tasks asynchronously
        for task in due_tasks:
            if self.user_manager.check_and_consume_quota(task.username):
                logger.info(
                    f"Dispatching task '{task.task_id}' for user '{task.username}'..."
                )
                await self.executor.run_task(task)


# ==========================================
# Usage / Demo
# ==========================================
async def main():
    # 1. Initialize System Components
    user_mgr = UserManager()
    executor = TaskExecutor()
    scheduler = SchedulerEngine(user_mgr, executor)

    # 2. Add Users (Quota Control)
    user_mgr.add_user("alice", quota=2)  # Quota capped at 2
    user_mgr.add_user("bob", quota=5)

    # 3. Add Tasks (Multiple tasks per user, configurable dictionary parameters)
    tasks_to_schedule = [
        Task(
            task_id="T01",
            username="alice",
            scheduled_time="12:00",
            action="sync",
            target="/data/x",
            params={"depth": "shallow"},
        ),
        Task(
            task_id="T02",
            username="bob",
            scheduled_time="12:00",
            action="backup",
            target="/srv/y",
            params={"compress": True},
        ),
        Task(
            task_id="T03",
            username="alice",
            scheduled_time="12:00",
            action="delete",
            target="/tmp/z",
            params={"force": True},
        ),
        # Alice's 3rd task at 12:00 — will exceed her quota of 2
        Task(
            task_id="T04",
            username="alice",
            scheduled_time="12:00",
            action="sync",
            target="/data/w",
            params={"depth": "full"},
        ),
    ]

    for task in tasks_to_schedule:
        scheduler.add_task(task)

    # 4. Trigger Execution (Simulating 12:00 PM run)
    await scheduler.execute_due_tasks(current_time="12:00")


if __name__ == "__main__":
    asyncio.run(main())
