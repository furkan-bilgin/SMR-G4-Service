from datetime import datetime

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class SMRG4Job(Base):
    __tablename__ = "smr_g4_jobs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(
        nullable=False, default=datetime.utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(nullable=True, default=None)

    config: Mapped[dict] = mapped_column(JSONB, nullable=False)
