from fastapi import Depends, FastAPI
from sqlalchemy.orm import Session

from .database import get_db
from .models import SMRG4Job
from .schemas import ScheduleJobRequest

app = FastAPI()


@app.post("/schedule")
async def schedule_job(job_config: ScheduleJobRequest, db: Session = Depends(get_db)):
    job = SMRG4Job(
        config=job_config.job_config,
    )
    db.add(job)
    db.commit()
    return {"success": True, "job_id": job.id}
