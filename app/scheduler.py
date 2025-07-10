from contextlib import asynccontextmanager

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

scheduler = BackgroundScheduler()
scheduler.start()


@scheduler.scheduled_job(CronTrigger(second="*"))
def scheduled_job():
    print("This job runs every second.")


@asynccontextmanager
async def lifespan(app):
    yield
    scheduler.shutdown()
