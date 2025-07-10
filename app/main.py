import asyncio
import json
import logging
import os
import shutil
import subprocess
import tempfile
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime

import zmq
from apscheduler.triggers.cron import CronTrigger
from fastapi import Depends, FastAPI
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from .database import get_db
from .models import SMRG4Job
from .scheduler import scheduler
from .schemas import ScheduleJobRequest
from .settings import (
    DOCKER_GEANT4_CONTAINER_ID,
    IPC_SMR_G4_PUBSUB,
    OUTPUT_PATH,
    SMR_G4_DOCKER_PATH,
    SMR_G4_REAL_PATH,
)


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


@app.get("/jobs/{job_id}/stream")
async def stream_job_output(job_id: int):
    """
    Stream the output of a job in real-time.
    """
    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(IPC_SMR_G4_PUBSUB)
    sub.setsockopt_string(zmq.SUBSCRIBE, str(job_id))

    async def message_generator():
        loop = asyncio.get_event_loop()
        try:
            while True:
                await loop.run_in_executor(None, sub.recv)
                message: G4Stdout = await loop.run_in_executor(None, sub.recv_pyobj)
                line = message.stdout_line or message.stderr_line
                if line:
                    yield line + "\n"
        finally:
            sub.close()
            ctx.term()

    return StreamingResponse(message_generator(), media_type="text/plain")


@dataclass(kw_only=True)
class G4Stdout:
    job_id: int
    stdout_line: str | None = None
    stderr_line: str | None = None


@scheduler.scheduled_job(CronTrigger(second="*"))
def process_scheduled_jobs():
    global g4_out_queue
    db = next(get_db())
    job = db.query(SMRG4Job).filter(SMRG4Job.completed_at.is_(None)).first()
    if not job:
        logging.info("No uncompleted jobs found.")
        return

    # Create temp file for job config
    temp_config_path = tempfile.mktemp(suffix=".json")
    with open(temp_config_path, "w") as f:
        json.dump(job.config, f)
    # Move temp file into SMR-G4's config path
    shutil.move(temp_config_path, f"{SMR_G4_REAL_PATH}/config/config.json")
    # Init ZMQ publisher
    ctx = zmq.Context()
    pub = ctx.socket(zmq.PUB)
    pub.bind(IPC_SMR_G4_PUBSUB)
    # Run SMR-G4 in the Docker container
    process = subprocess.Popen(
        [
            "docker",
            "exec",
            DOCKER_GEANT4_CONTAINER_ID,
            "/opt/bashrc.sh",
            "/bin/bash",
            "-c",
            f"cd {SMR_G4_DOCKER_PATH} && ./SMR-G4 ./macros/run.mac",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    job_output = []
    for line in process.stdout:
        line = line.strip()
        pub.send(str(job.id).encode(), zmq.SNDMORE)
        pub.send_pyobj(G4Stdout(job_id=job.id, stdout_line=line))
        job_output.append(line)
    for line in process.stderr:
        line = line.strip()
        pub.send(str(job.id).encode(), zmq.SNDMORE)
        pub.send_pyobj(G4Stdout(job_id=job.id, stderr_line=line))
        job_output.append(line)
    process.wait()
    pub.close()
    if process.returncode != 0:
        logging.error(f"Job {job.id} failed with return code {process.returncode}")
        return
    # Copy all output files
    for file in os.listdir(f"{SMR_G4_REAL_PATH}/output"):
        src = os.path.join(SMR_G4_REAL_PATH, "output", file)
        dst = os.path.join(OUTPUT_PATH, str(job.id), file)
        if not os.path.exists(os.path.dirname(dst)):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy(src, dst)

    logging.info(f"Job {job.id} completed successfully.")
    job.completed_at = datetime.utcnow()
    job.job_output = "\n".join(job_output).encode("utf-8")
    db.commit()
