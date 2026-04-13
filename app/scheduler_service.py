from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from zoneinfo import ZoneInfo

from .config import settings
from .db import add_run_log
from .service import FollowupReminderService


class SchedulerService:
    def __init__(self, service: FollowupReminderService) -> None:
        self.service = service
        self.scheduler = BackgroundScheduler(timezone=ZoneInfo(settings.timezone))

    def job_initial_check(self) -> None:
        if not settings.auto_run_enabled:
            add_run_log("job_skip", "job_initial_check", True, detail="AUTO_RUN_ENABLED=false")
            return
        self.service.run_initial_check()

    def job_urge_cycle(self) -> None:
        if not settings.auto_run_enabled:
            add_run_log("job_skip", "job_urge_cycle", True, detail="AUTO_RUN_ENABLED=false")
            return
        now = self.service.now()
        if now.hour == settings.initial_check_hour and now.minute == settings.initial_check_minute:
            return
        self.service.run_urge_cycle()

    def start(self) -> None:
        self.scheduler.add_job(
            self.job_initial_check,
            CronTrigger(hour=settings.initial_check_hour, minute=settings.initial_check_minute),
            id="initial_check",
            replace_existing=True,
        )
        self.scheduler.add_job(
            self.job_urge_cycle,
            CronTrigger(
                hour=f"{settings.initial_check_hour}-{settings.urge_end_hour}",
                minute=f"*/{settings.urge_interval_minutes}",
            ),
            id="urge_cycle",
            replace_existing=True,
        )
        self.scheduler.start()

    def shutdown(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
