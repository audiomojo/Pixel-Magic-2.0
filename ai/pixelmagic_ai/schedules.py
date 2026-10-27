"""The AI worker's Temporal schedules; create-or-update like src/schedules.ts."""

import os
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleCalendarSpec,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleRange,
    ScheduleSpec,
    ScheduleUpdate,
    ScheduleUpdateInput,
)

from .config import Settings
from .workflows import TRAIN_WORKFLOW_ID

TASK_QUEUE = "ai"
DESCRIBE_EVERY = timedelta(minutes=15)
TRAIN_HOUR = 3  # local time


def _valid_zone(name: str) -> bool:
    try:
        ZoneInfo(name)
    except (KeyError, ValueError, OSError):  # ZoneInfoNotFoundError is a KeyError
        return False
    return True


def local_time_zone(localtime: Path = Path("/etc/localtime")) -> str:
    """The machine's IANA zone name: $TZ when valid, else the /etc/localtime link target
    (".../zoneinfo/America/Denver"), else UTC."""
    tz = os.environ.get("TZ", "").removeprefix(":")
    if tz and _valid_zone(tz):
        return tz
    try:
        target = os.readlink(localtime)
    except OSError:
        return "UTC"
    _, sep, name = target.partition("zoneinfo/")
    return name if sep and _valid_zone(name) else "UTC"


def schedule_definitions(s: Settings) -> list[tuple[str, Schedule]]:
    """The schedules this deployment wants, from the current settings."""
    return [
        (
            "ai-describe",
            Schedule(
                action=ScheduleActionStartWorkflow(
                    "DescribeWorkflow",
                    args=[s.describe_concurrency],
                    id="ai-describe",
                    task_queue=TASK_QUEUE,
                ),
                spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=DESCRIBE_EVERY)]),
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            ),
        ),
        (
            "ai-train-nightly",
            Schedule(
                action=ScheduleActionStartWorkflow(
                    "TrainWorkflow", id=TRAIN_WORKFLOW_ID, task_queue=TASK_QUEUE
                ),
                spec=ScheduleSpec(
                    calendars=[
                        ScheduleCalendarSpec(
                            hour=[ScheduleRange(TRAIN_HOUR)],
                            minute=[ScheduleRange(0)],
                            second=[ScheduleRange(0)],
                        )
                    ],
                    time_zone_name=local_time_zone(),
                ),
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            ),
        ),
    ]


async def ensure_schedules(client: Client, s: Settings) -> None:
    """Creates the schedules, or updates existing ones in place so changed settings take
    effect. The live state is kept (an operator may have paused one)."""
    for schedule_id, wanted in schedule_definitions(s):
        try:
            await client.create_schedule(schedule_id, wanted)
        except ScheduleAlreadyRunningError:

            def update(inp: ScheduleUpdateInput, wanted: Schedule = wanted) -> ScheduleUpdate:
                prev = inp.description.schedule
                return ScheduleUpdate(
                    schedule=Schedule(
                        action=wanted.action, spec=wanted.spec, policy=wanted.policy,
                        state=prev.state,
                    )
                )

            await client.get_schedule_handle(schedule_id).update(update)
