import logging
import time
from contextlib import asynccontextmanager

from apscheduler.triggers.cron import CronTrigger
from fastapi import Depends, FastAPI
from sqlalchemy.orm import Session

from .database import get_db
from .models import SMRG4Job
from .scheduler import scheduler
from .schemas import ScheduleJobRequest


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(lifespan=lifespan)
logging.getLogger("apscheduler.scheduler").setLevel(logging.ERROR)


@app.post("/schedule")
async def schedule_job(job_config: ScheduleJobRequest, db: Session = Depends(get_db)):
    job = SMRG4Job(
        config=job_config.job_config,
    )
    db.add(job)
    db.commit()
    return {"success": True, "job_id": job.id}


@app.get("/jobs")
async def list_jobs(db: Session = Depends(get_db)):
    jobs = db.query(SMRG4Job).all()
    return {"jobs": jobs}


@app.get("/jobs/{job_id}")
async def get_job(job_id: int, db: Session = Depends(get_db)):
    job = db.query(SMRG4Job).filter(SMRG4Job.id == job_id).first()
    if not job:
        return {"error": "Job not found"}, 404

    return {"job": job}


@scheduler.scheduled_job(CronTrigger(second="*"))
def process_scheduled_jobs():
    db = next(get_db())
    job = db.query(SMRG4Job).filter(SMRG4Job.completed_at.is_(None)).first()
    if not job:
        logging.info("No uncompleted jobs found.")
        return
