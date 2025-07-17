import asyncio
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
import traceback
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime

import zmq
from apscheduler.triggers.cron import CronTrigger
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import StaleDataError

from .database import SessionLocal, get_db
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

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(lifespan=lifespan)

logging.getLogger("apscheduler.executors").setLevel(logging.ERROR)
logging.getLogger("apscheduler.scheduler").setLevel(logging.ERROR)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/schedule")
async def schedule_job(job_config: ScheduleJobRequest, db: Session = Depends(get_db)):
    logging.info("Scheduling a new job with config: %s", job_config)
    job = SMRG4Job(
        config=job_config.job_config,
    )
    db.add(job)
    db.commit()
    logging.info(f"Job scheduled with ID: {job.id}")
    return {"success": True, "job_id": job.id}


@app.get("/jobs")
async def list_jobs(db: Session = Depends(get_db)):
    logging.info("Listing all jobs")
    jobs = db.query(SMRG4Job).order_by(SMRG4Job.created_at.desc()).all()
    logging.info(f"Found {len(jobs)} jobs")
    return {"jobs": jobs}


@app.get("/jobs/{job_id}")
async def get_job(job_id: int, db: Session = Depends(get_db)):
    logging.info(f"Fetching job {job_id}")
    job = db.query(SMRG4Job).filter(SMRG4Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    logging.info(f"Returning job {job_id}")
    return {"job": job}


@app.delete("/jobs/{job_id}")
async def delete_job(job_id: int, db: Session = Depends(get_db)):
    job = db.query(SMRG4Job).filter(SMRG4Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    db.delete(job)
    db.commit()
    # delete files at OUTPUT_PATH
    job_output_path = os.path.join(OUTPUT_PATH, str(job_id))
    if os.path.exists(job_output_path):
        shutil.rmtree(job_output_path)
    tar_path = f"{job_output_path}.tar.gz"
    if os.path.exists(tar_path):
        os.remove(tar_path)
    return {"success": True}


@app.post("/jobs/{job_id}/delete")
async def delete_job_post(job_id: int, db: Session = Depends(get_db)):
    """Delete job subsitute with POST, because Erciyes network prohibits DELETE method."""
    return await delete_job(job_id, db)


@app.get("/jobs/{job_id}/stream")
async def stream_job_output(job_id: int):
    """
    Stream the output of a job in real-time.
    """
    logging.info(f"Starting streaming for job {job_id}")
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
            logging.info(f"Stopping streaming for job {job_id}")
            sub.close()
            ctx.term()

    return StreamingResponse(message_generator(), media_type="text/plain")


@app.get("/jobs/{job_id}/download/tar")
async def download_job_output(job_id: int, db: Session = Depends(get_db)):
    """
    Download the output of a job as a tar.gz file.
    """
    logging.info(f"Downloading output for job {job_id}")
    job = db.query(SMRG4Job).filter(SMRG4Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    job_output_path = os.path.join(OUTPUT_PATH, str(job_id))
    if not os.path.exists(job_output_path):
        raise HTTPException(status_code=404, detail="Job output not found")

    tar_path = f"{job_output_path}.tar.gz"
    if not os.path.exists(tar_path) or os.path.getmtime(tar_path) < os.path.getmtime(
        job_output_path
    ):
        shutil.make_archive(job_output_path, "gztar", job_output_path)
    logging.info(f"Tar file created at {tar_path}")
    return FileResponse(
        tar_path, media_type="application/gzip", filename=f"job_{job_id}_output.tar.gz"
    )


@app.get("/output/{job_id}/{filename:path}")
async def get_output_file(job_id: int, filename: str, db: Session = Depends(get_db)):
    logging.info(f"Fetching output file {filename} for job {job_id}")
    job = db.query(SMRG4Job).filter(SMRG4Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    file_path = os.path.join(OUTPUT_PATH, str(job_id), filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    logging.info(f"Returning file {file_path}")
    return FileResponse(os.path.join(OUTPUT_PATH, str(job_id), filename))


@dataclass(kw_only=True)
class G4Stdout:
    job_id: int
    stdout_line: str | None = None
    stderr_line: str | None = None


@scheduler.scheduled_job(CronTrigger(second="*"))
def process_scheduled_jobs():
    db = SessionLocal()
    try:
        job = db.query(SMRG4Job).filter(SMRG4Job.completed_at.is_(None)).first()
        if not job:
            return
        job.is_processing = True
        db.commit()
        JOB_SOURCE_PATH = os.path.join(SMR_G4_REAL_PATH, "output")
        JOB_DEST_PATH = os.path.join(OUTPUT_PATH, str(job.id))
        if not os.path.exists(JOB_DEST_PATH):
            os.makedirs(JOB_DEST_PATH, exist_ok=True)

        # Create temp file for job config
        temp_config_path = tempfile.mktemp(suffix=".json")
        with open(temp_config_path, "w") as f:
            json.dump(job.config, f)

        # Move temp file into SMR-G4's config path
        shutil.move(temp_config_path, f"{SMR_G4_REAL_PATH}/config/config.json")

        # Change run.mac to set /run/beamOn {config.event_count}
        with open(f"{SMR_G4_REAL_PATH}/macros/run.mac", "r") as f:
            run_mac_content = f.read()
        run_mac_content = re.sub(
            r"(/run/beamOn\s+)\d+",
            lambda m: f"{m.group(1)}{job.config['event_count']}",
            run_mac_content,
        )
        with open(f"{SMR_G4_REAL_PATH}/macros/run.mac", "w") as f:
            f.write(run_mac_content)

        # Init ZMQ publisher
        ctx = zmq.Context()
        pub = ctx.socket(zmq.PUB)
        pub.bind(IPC_SMR_G4_PUBSUB)

        job_output = []

        def pub_send(message: G4Stdout):
            pub.send_string(str(message.job_id), zmq.SNDMORE)
            pub.send_pyobj(message)
            job_output.append(message.stdout_line or message.stderr_line or "")

        def run(command: str) -> subprocess.Popen:
            return subprocess.Popen(
                [
                    "docker",
                    "exec",
                    DOCKER_GEANT4_CONTAINER_ID,
                    "/opt/bashrc.sh",
                    "/bin/bash",
                    "-c",
                    f"cd {SMR_G4_DOCKER_PATH} && {command}",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

        time.sleep(1)  # Allow time for the publisher to bind
        # Render the geometry
        pub_send(G4Stdout(job_id=job.id, stdout_line="Rendering geometry..."))
        process = run(
            "export G4DAWNFILE_DEST_DIR='./output/' && ./SMR-G4 ./macros/render.mac"
        )
        process.wait(timeout=30)
        if process.returncode != 0:
            raise RuntimeError(
                f"Initial geometry rendering failed with return code {process.returncode}\n{process.stdout.read()} {process.stderr.read()}"
            )
        # Copy the latest .prim file to the output directory
        latest_prim_file = max(
            [
                os.path.join(JOB_SOURCE_PATH, f)
                for f in os.listdir(JOB_SOURCE_PATH)
                if f.endswith(".prim")
            ],
            key=os.path.getctime,
        )
        shutil.move(latest_prim_file, f"{JOB_DEST_PATH}/geometry.prim")
        pub_send(G4Stdout(job_id=job.id, stdout_line="Geometry rendered successfully."))

        # Run SMR-G4 in the Docker container
        process = run("./SMR-G4 ./macros/run.mac")

        for line in process.stdout:
            line = line.strip()
            pub_send(G4Stdout(job_id=job.id, stdout_line=line))

        for line in process.stderr:
            line = line.strip()
            pub_send(G4Stdout(job_id=job.id, stderr_line=line))

        while True:
            try:
                process.wait(timeout=2)
                break
            except subprocess.TimeoutExpired:
                if not db.query(SMRG4Job).get(job.id):
                    logging.info(f"Job {job.id} has been deleted, stopping processing.")
                    process.kill()
                    return

        if process.returncode != 0:
            raise RuntimeError(
                f"Job {job.id} failed with return code {process.returncode}"
            )

        # Copy CSV files
        for file in os.listdir(JOB_SOURCE_PATH):
            if not file.endswith(".csv"):
                continue
            src = os.path.join(JOB_SOURCE_PATH, file)
            dst = os.path.join(JOB_DEST_PATH, file)
            shutil.copy(src, dst)

        job.completed_at = datetime.utcnow()
        job.is_processing = False
        pub_send(G4Stdout(job_id=job.id, stdout_line="Job completed successfully."))
        job.job_output = "\n".join(job_output).encode("utf-8")
        db.commit()
        pub.close()
        logging.info(f"Job {job.id} completed successfully.")
    except Exception as e:
        logging.error(
            f"Job {job.id} failed with exception {e} {traceback.format_exc()}"
        )
        try:
            job.is_processing = False
            db.commit()
        except StaleDataError:
            pass
    finally:
        db.close()
