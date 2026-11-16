from sqlalchemy import String, JSON, Double, ForeignKey, Boolean, Integer
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base


class Flow(Base):
    """一条可复用的加工流程；定义按版本留存，当前发布指针原子切换。"""
    __tablename__ = 'flows'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(160), unique=True)
    created_by: Mapped[int] = mapped_column(ForeignKey('users.id'))
    created_at: Mapped[float] = mapped_column(Double)
    current_publication_id: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)


class FlowRevision(Base):
    """流程定义的不可变版本；运行始终绑定某一版本，执行中改版留给下次。"""
    __tablename__ = 'flow_revisions'
    id: Mapped[int] = mapped_column(primary_key=True)
    flow_id: Mapped[int] = mapped_column(ForeignKey('flows.id'))
    revision: Mapped[int] = mapped_column(Integer)
    definition: Mapped[dict] = mapped_column(JSON, default=dict)
    note: Mapped[str] = mapped_column(String(400), default='')
    created_by: Mapped[int] = mapped_column(ForeignKey('users.id'))
    created_at: Mapped[float] = mapped_column(Double)


class PipelineRun(Base):
    """一次运行：启动时锁定流程版本与输入批次指纹。"""
    __tablename__ = 'pipeline_runs'
    id: Mapped[int] = mapped_column(primary_key=True)
    flow_id: Mapped[int] = mapped_column(ForeignKey('flows.id'))
    revision_id: Mapped[int] = mapped_column(ForeignKey('flow_revisions.id'))
    status: Mapped[str] = mapped_column(String(20), default='queued')
    inputs: Mapped[list] = mapped_column(JSON, default=list)
    error: Mapped[str] = mapped_column(String(1000), default='')
    created_by: Mapped[int] = mapped_column(ForeignKey('users.id'))
    created_at: Mapped[float] = mapped_column(Double)
    started_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    finished_at: Mapped[float | None] = mapped_column(Double, nullable=True)


class StepRun(Base):
    """运行内的单个步骤；attempt 用作围栏令牌，仅接纳当前尝试的产物。"""
    __tablename__ = 'step_runs'
    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey('pipeline_runs.id'))
    step_key: Mapped[str] = mapped_column(String(60))
    name: Mapped[str] = mapped_column(String(160))
    kind: Mapped[str] = mapped_column(String(30))
    params: Mapped[dict] = mapped_column(JSON, default=dict)
    parents: Mapped[list] = mapped_column(JSON, default=list)
    is_exit: Mapped[bool] = mapped_column(Boolean, default=False)
    fingerprint: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(20), default='pending')
    attempt: Mapped[int] = mapped_column(Integer, default=0)
    claimed_by: Mapped[str] = mapped_column(String(120), default='')
    batch_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str] = mapped_column(String(1000), default='')
    started_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    finished_at: Mapped[float | None] = mapped_column(Double, nullable=True)


class Batch(Base):
    """中间批次：按内容指纹寻址，跨运行去重，重试与无关分支直接复用。"""
    __tablename__ = 'batches'
    id: Mapped[int] = mapped_column(primary_key=True)
    fingerprint: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    kind: Mapped[str] = mapped_column(String(30))
    records: Mapped[list] = mapped_column(JSON, default=list)
    rows: Mapped[int] = mapped_column(Integer, default=0)
    checksum: Mapped[str] = mapped_column(String(64))
    run_id: Mapped[int] = mapped_column(ForeignKey('pipeline_runs.id'))
    step_key: Mapped[str] = mapped_column(String(60))
    created_at: Mapped[float] = mapped_column(Double)


class Publication(Base):
    """一次发布：全部出口通过校验后一次性切换，失败保留旧版。"""
    __tablename__ = 'publications'
    id: Mapped[int] = mapped_column(primary_key=True)
    flow_id: Mapped[int] = mapped_column(ForeignKey('flows.id'))
    run_id: Mapped[int] = mapped_column(ForeignKey('pipeline_runs.id'))
    exits: Mapped[list] = mapped_column(JSON, default=list)
    created_at: Mapped[float] = mapped_column(Double)
