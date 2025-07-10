from pydantic import BaseModel


class ScheduleJobRequest(BaseModel):
    job_config: dict
