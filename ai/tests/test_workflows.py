import asyncio
import uuid
from pathlib import Path

import pytest
from temporalio import activity
from temporalio.client import (
    Client,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
)
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from pixelmagic_ai import schedules
from pixelmagic_ai.config import Settings
from pixelmagic_ai.workflows import PAGE_SIZE, DescribeWorkflow

QUEUE = "ai-test"


class FakeAi:
    """In-memory backlog. Pages are shorter than PAGE_SIZE so 450 items take 9 pages and the
    workflow must continue-as-new (every 5 pages) to finish."""

    def __init__(self, n: int, page: int = 50, broken: frozenset[str] = frozenset()) -> None:
        self.backlog = [(f"{i:064x}", "jpg") for i in range(n)]
        self.page = page
        self.broken = broken
        self.described: list[str] = []
        self.limits: list[int] = []
        self.in_flight = 0
        self.max_in_flight = 0
        self.ranked = 0
        self.attempts: dict[str, list[int]] = {}

    @activity.defn(name="describe_backlog_page")
    async def describe_backlog_page(
        self, limit: int, after_sha: str | None = None
    ) -> list[tuple[str, str]]:
        self.limits.append(limit)
        rest = [it for it in self.backlog if after_sha is None or it[0] > after_sha]
        return rest[: min(limit, self.page)]

    @activity.defn(name="describe_photo")
    async def describe_photo(self, sha256: str, ext: str) -> str:
        self.attempts.setdefault(sha256, []).append(activity.info().attempt)
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            await asyncio.sleep(0.002)
            if sha256 in self.broken:
                raise OSError("share went away")
            self.described.append(sha256)
            self.backlog.remove((sha256, ext))
            return "ok"
        finally:
            self.in_flight -= 1

    @activity.defn(name="rank_sets")
    async def rank_sets(self) -> int:
        self.ranked += 1
        return 0


async def _run(fake: FakeAi, concurrency: int):
    async with await WorkflowEnvironment.start_time_skipping() as env, Worker(
        env.client,
        task_queue=QUEUE,
        workflows=[DescribeWorkflow],
        activities=[fake.describe_backlog_page, fake.describe_photo, fake.rank_sets],
    ):
        handle = await env.client.start_workflow(
            DescribeWorkflow.run, concurrency, id=f"describe-{uuid.uuid4()}", task_queue=QUEUE
        )
        result = await handle.result()
        last = await env.client.get_workflow_handle(handle.id).describe()
        return result, handle.first_execution_run_id, last.run_id


def run(fake: FakeAi, concurrency: int):
    """(result, first run id, last run id) of one DescribeWorkflow chain."""
    return asyncio.run(_run(fake, concurrency))


def test_every_item_described_once_across_continue_as_new():
    fake = FakeAi(450)
    result, first_run, last_run = run(fake, concurrency=3)
    assert result == 450
    assert sorted(fake.described) == sorted(f"{i:064x}" for i in range(450))
    assert len(set(fake.described)) == 450
    assert set(fake.limits) == {PAGE_SIZE}
    assert first_run != last_run  # continued as new
    assert fake.ranked == 1


def test_rank_sets_skipped_when_nothing_described():
    fake = FakeAi(0)
    result, first_run, last_run = run(fake, concurrency=2)
    assert result == 0
    assert fake.ranked == 0
    assert first_run == last_run


@pytest.mark.parametrize("concurrency", [1, 2, 4])
def test_concurrency_never_exceeds_setting(concurrency):
    fake = FakeAi(40, page=40)
    run(fake, concurrency=concurrency)
    assert fake.max_in_flight == concurrency


def test_a_photo_that_keeps_failing_does_not_stop_the_rest():
    stuck = f"{0:064x}"
    fake = FakeAi(120, page=50, broken=frozenset({stuck}))
    result = run(fake, concurrency=2)[0]
    assert result == 119
    assert stuck not in fake.described
    assert fake.backlog == [(stuck, "jpg")]
    assert fake.attempts[stuck] == [1, 2, 3, 4, 5]  # one activity (5 attempts) per run
    assert fake.ranked == 1


def test_more_stuck_photos_than_a_page_do_not_hide_the_rest():
    """60 stuck photos sort before 40 good ones; pages of 20 (3 pages of stuck only)."""
    stuck = frozenset(f"{i:064x}" for i in range(60))
    fake = FakeAi(100, page=20, broken=stuck)
    result, first_run, last_run = run(fake, concurrency=4)
    assert result == 40
    assert sorted(fake.described) == [f"{i:064x}" for i in range(60, 100)]
    assert sorted(fake.backlog) == sorted((s, "jpg") for s in stuck)
    assert all(fake.attempts[s] == [1, 2, 3, 4, 5] for s in stuck)
    assert first_run != last_run  # 5 pages, then continue-as-new carried the cursor
    assert fake.ranked == 1


def settings(concurrency: int = 2) -> Settings:
    return Settings(Path("/x"), "postgres://x", "127.0.0.1:7233", "default", concurrency)


def test_schedule_definitions():
    (sid, sched), = schedules.schedule_definitions(settings(3))
    assert sid == "ai-describe"
    assert sched.action.workflow == "DescribeWorkflow"
    assert sched.action.args == [3]
    assert sched.action.task_queue == "ai"
    assert sched.action.id == "ai-describe"
    assert sched.spec.intervals == [ScheduleIntervalSpec(every=schedules.DESCRIBE_EVERY)]
    assert schedules.DESCRIBE_EVERY.total_seconds() == 900
    assert sched.policy.overlap == ScheduleOverlapPolicy.SKIP


def test_ensure_schedules_creates_then_updates_keeping_pause():
    asyncio.run(_ensure_schedules_creates_then_updates_keeping_pause())


async def _ensure_schedules_creates_then_updates_keeping_pause():
    async with await WorkflowEnvironment.start_local() as local:
        client: Client = local.client
        await schedules.ensure_schedules(client, settings(2))
        handle = client.get_schedule_handle("ai-describe")
        await handle.pause(note="operator")
        await schedules.ensure_schedules(client, settings(5))
        desc = await handle.describe()
        args = client.data_converter.payload_converter.from_payloads(desc.schedule.action.args)
        assert args == [5]
        assert desc.schedule.spec.intervals == [ScheduleIntervalSpec(every=schedules.DESCRIBE_EVERY)]
        assert desc.schedule.state.paused
        assert desc.schedule.state.note == "operator"
