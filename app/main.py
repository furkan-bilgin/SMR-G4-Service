import asyncio
import asyncio.subprocess as aiosubprocess
import json
import logging
import os
import re
import shutil
import tempfile
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime

import aiofiles
import zmq
import zmq.asyncio as azmq
from apscheduler.triggers.cron import CronTrigger
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import Session

from .database import AsyncSessionLocal, get_db
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
    level=logging.INFO,
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
async def schedule_job(
    job_config: ScheduleJobRequest, db: AsyncSession = Depends(get_db)
):
    logging.info("Scheduling a new job with config: %s", job_config)
    job = SMRG4Job(
        config=job_config.job_config,
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)
    logging.info(f"Job scheduled with ID: {job.id}")
    return {"success": True, "job_id": job.id}


@app.get("/jobs")
async def list_jobs(db: AsyncSession = Depends(get_db)):
    logging.info("Listing all jobs")
    result = await db.execute(select(SMRG4Job).order_by(SMRG4Job.created_at.desc()))
    jobs = result.scalars().all()
    logging.info(f"Found {len(jobs)} jobs")
    return {"jobs": jobs}


@app.get("/jobs/{job_id}")
async def get_job(job_id: int, db: AsyncSession = Depends(get_db)):
    logging.info(f"Fetching job {job_id}")
    result = await db.execute(select(SMRG4Job).filter(SMRG4Job.id == job_id))
    job = result.scalars().first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    logging.info(f"Returning job {job_id}")
    return {"job": job}


@app.delete("/jobs/{job_id}")
async def delete_job(job_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(SMRG4Job).filter(SMRG4Job.id == job_id))
    job = result.scalars().first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    await db.delete(job)
    await db.commit()
    # delete files at OUTPUT_PATH
    job_output_path = os.path.join(OUTPUT_PATH, str(job_id))
    if os.path.exists(job_output_path):
        shutil.rmtree(job_output_path)
    tar_path = f"{job_output_path}.tar.gz"
    if os.path.exists(tar_path):
        os.remove(tar_path)
    return {"success": True}


@app.post("/jobs/{job_id}/delete")
async def delete_job_post(job_id: int, db: AsyncSession = Depends(get_db)):
    """Delete job substitute with POST, because Erciyes network prohibits DELETE method."""
    return await delete_job(job_id, db)


@app.get("/jobs/{job_id}/stream")
async def stream_job_output(job_id: int):
    """
    Stream the output of a job in real-time.
    """
    logging.info(f"Starting streaming for job {job_id}")
    ctx = azmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(IPC_SMR_G4_PUBSUB)
    sub.setsockopt_string(zmq.SUBSCRIBE, str(job_id))

    async def message_generator():
        try:
            while True:
                await sub.recv()  # topic
                message: G4Stdout = await sub.recv_pyobj()
                line = message.stdout_line or message.stderr_line
                if line:
                    if isinstance(line, bytes):
                        line = line.decode("utf-8")
                    yield (line + "\n").encode("utf-8")
        finally:
            logging.info(f"Stopping streaming for job {job_id}")
            sub.close()
            ctx.term()

    return StreamingResponse(message_generator(), media_type="text/plain")


@app.get("/jobs/{job_id}/download/tar")
async def download_job_output(job_id: int, db: AsyncSession = Depends(get_db)):
    """
    Download the output of a job as a tar.gz file.
    """
    logging.info(f"Downloading output for job {job_id}")
    result = await db.execute(select(SMRG4Job).filter(SMRG4Job.id == job_id))
    job = result.scalars().first()
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
async def get_output_file(
    job_id: int, filename: str, session: Session = Depends(get_db)
):
    logging.info(f"Fetching output file {filename} for job {job_id}")
    result = await session.execute(select(SMRG4Job).filter(SMRG4Job.id == job_id))
    job = result.scalars().first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    file_path = os.path.join(OUTPUT_PATH, str(job_id), filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    logging.info(f"Returning file {file_path}")
    return FileResponse(file_path)


@dataclass(kw_only=True)
class G4Stdout:
    job_id: int
    stdout_line: str | None = None
    stderr_line: str | None = None


async def _do_process_scheduled_job(job: SMRG4Job, session: AsyncSession):
    job.is_processing = True
    await session.commit()

    JOB_SOURCE_PATH = os.path.join(SMR_G4_REAL_PATH, "output")
    JOB_DEST_PATH = os.path.join(OUTPUT_PATH, str(job.id))

    if not os.path.exists(JOB_DEST_PATH):
        await asyncio.to_thread(os.makedirs, JOB_DEST_PATH, True)

    # Create temp file for job config
    temp_config_path = tempfile.mktemp(suffix=".json")
    async with aiofiles.open(temp_config_path, "w") as f:
        await f.write(json.dumps(job.config))

    # Move temp file into SMR-G4's config path
    await asyncio.to_thread(
        shutil.move, temp_config_path, f"{SMR_G4_REAL_PATH}/config/config.json"
    )

    # Change run.mac to set /run/beamOn {config.event_count}
    run_mac_path = f"{SMR_G4_REAL_PATH}/macros/run.mac"
    async with aiofiles.open(run_mac_path, "r") as f:
        run_mac_content = await f.read()

    run_mac_content = re.sub(
        r"(/run/beamOn\s+)\d+",
        lambda m: f"{m.group(1)}{job.config['event_count']}",
        run_mac_content,
    )
    async with aiofiles.open(run_mac_path, "w") as f:
        await f.write(run_mac_content)

    ctx = azmq.Context()
    pub = ctx.socket(zmq.PUB)
    pub.bind(IPC_SMR_G4_PUBSUB)

    job_output = []

    async def pub_send(message: G4Stdout):
        await pub.send_string(str(message.job_id), zmq.SNDMORE)
        await pub.send_pyobj(message)
        job_output.append(message.stdout_line or message.stderr_line or "")

    async def run_docker_command(command: str) -> aiosubprocess.Process:
        return await aiosubprocess.create_subprocess_exec(
            "docker",
            "exec",
            DOCKER_GEANT4_CONTAINER_ID,
            "/opt/bashrc.sh",
            "/bin/bash",
            "-c",
            f"cd {SMR_G4_DOCKER_PATH} && {command}",
            stdout=aiosubprocess.PIPE,
            stderr=aiosubprocess.PIPE,
        )

    await asyncio.sleep(1)

    await pub_send(
        G4Stdout(
            job_id=job.id,
            stdout_line="Received config:\n" + json.dumps(job.config, indent=2),
        )
    )
    await pub_send(G4Stdout(job_id=job.id, stdout_line="Rendering geometry..."))

    process = await run_docker_command(
        "export G4DAWNFILE_DEST_DIR='./output/' && ./SMR-G4 ./macros/render.mac"
    )

    try:
        await asyncio.wait_for(process.wait(), timeout=30)
    except asyncio.TimeoutError:
        process.kill()
        raise RuntimeError("Geometry rendering timed out.")

    if process.returncode != 0:
        stdout_data = await process.stdout.read()
        stderr_data = await process.stderr.read()
        raise RuntimeError(
            f"Initial geometry rendering failed with return code {process.returncode}\n{stdout_data} {stderr_data}"
        )

    prim_files = [
        os.path.join(JOB_SOURCE_PATH, f)
        for f in await asyncio.to_thread(os.listdir, JOB_SOURCE_PATH)
        if f.endswith(".prim")
    ]

    latest_prim_file = max(prim_files, key=os.path.getctime)

    await asyncio.to_thread(os.makedirs, JOB_DEST_PATH, exist_ok=True)
    await asyncio.to_thread(os.chmod, JOB_DEST_PATH, 0o777)
    await asyncio.to_thread(
        shutil.move, latest_prim_file, f"{JOB_DEST_PATH}/geometry.prim"
    )
    await pub_send(
        G4Stdout(job_id=job.id, stdout_line="Geometry rendered successfully.")
    )

    # Run SMR-G4 in the Docker container
    process = await run_docker_command("./SMR-G4 ./macros/run.mac")

    async def read_stream(stream, is_stdout=True):
        while True:
            line = await stream.readline()
            if not line:
                break
            line = line.strip()
            if isinstance(line, bytes):
                line = line.decode("utf-8")
            if is_stdout:
                await pub_send(G4Stdout(job_id=job.id, stdout_line=line))
            else:
                await pub_send(G4Stdout(job_id=job.id, stderr_line=line))

    await asyncio.gather(
        read_stream(process.stdout, True),
        read_stream(process.stderr, False),
    )

    while True:
        try:
            await asyncio.wait_for(process.wait(), timeout=2)
            break
        except asyncio.TimeoutError:
            job_exists_result = await session.execute(
                select(SMRG4Job).filter(SMRG4Job.id == job.id)
            )
            job_exists = job_exists_result.scalars().first()
            if not job_exists:
                logging.info(f"Job {job.id} has been deleted, stopping processing.")
                process.kill()
                return

    if process.returncode != 0:
        raise RuntimeError(f"Job {job.id} failed with return code {process.returncode}")

    files = await asyncio.to_thread(os.listdir, JOB_SOURCE_PATH)
    for file in files:
        if not file.endswith(".csv"):
            continue
        src = os.path.join(JOB_SOURCE_PATH, file)
        dst = os.path.join(JOB_DEST_PATH, file)
        await asyncio.to_thread(shutil.copy, src, dst)

    job.completed_at = datetime.utcnow()
    job.is_processing = False
    await pub_send(G4Stdout(job_id=job.id, stdout_line="Job completed successfully."))
    job.job_output = "\n".join(job_output).encode("utf-8")
    await session.commit()
    pub.close()
    logging.info(f"Job {job.id} completed successfully.")


@scheduler.scheduled_job(CronTrigger(second="*"))
async def process_scheduled_jobs():
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(SMRG4Job).filter(SMRG4Job.completed_at.is_(None))
        )
        job = result.scalars().first()
        if not job:
            return
        try:
            await _do_process_scheduled_job(job, session)
        except Exception as e:
            logging.error(f"Job failed with error: {e}")
            job.is_processing = False
            await session.commit()
