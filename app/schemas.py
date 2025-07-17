from pydantic import BaseModel


class ScheduleJobRequest(BaseModel):
    job_config: dict

    class Config:
        validate_by_name = True
        alias_generator = None
