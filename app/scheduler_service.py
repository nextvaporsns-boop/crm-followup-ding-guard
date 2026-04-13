from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from zoneinfo import ZoneInfo

from .config import settings
from .db import add_run_log, get_auto_schedule_enabled
from .service import FollowupReminderService


class SchedulerService:
    def __init__(self, service: FollowupReminderService) -> None:
        self.service = service
        self.scheduler = BackgroundScheduler(timezone=ZoneInfo(settings.timezone))

    def job_group_hourly(self) -> None:
        if not get_auto_schedule_enabled():
            add_run_log("job_skip", "job_group_hourly", True, detail="auto schedule disabled")
            return
        try:
            result = self.service.send_group_demo(source="scheduler_group_hourly")
            add_run_log(
                "scheduler_group_send",
                "job_group_hourly",
                True,
                detail=f"targets={result['target_count']}, preview={result['preview_names']}",
            )
        except Exception as exc:
            add_run_log("scheduler_group_send", "job_group_hourly", False, detail=str(exc))

    def start(self) -> None:
        self.scheduler.add_job(
            self.job_group_hourly,
            CronTrigger(hour=f"{settings.initial_check_hour}-{settings.urge_end_hour}", minute=settings.initial_check_minute),
            id="group_hourly",
            replace_existing=True,
        )
        self.scheduler.start()

    def shutdown(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
