from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from src.config.database import get_db
from src.models import Flow, FlowRevision, PipelineRun, StepRun, Batch, Publication
from src.schemas.pipeline import FlowInput, RevisionInput
from src.middlewares.auth import current_user, can_write
from src.services import pipeline
from src.services.audit import record
from src.utils.serialization import (flow_dict, revision_dict, run_dict, step_run_dict,
                                     batch_dict, publication_dict)

router = APIRouter(prefix='/pipeline')


def _get_flow(db, flow_id):
    flow = db.get(Flow, flow_id)
    if not flow:
        raise HTTPException(404, '加工流程不存在')
    return flow


def _publication_summary(db, publication_id):
    if not publication_id:
        return None
    p = db.get(Publication, publication_id)
    return publication_dict(p) if p else None


@router.get('/flows')
def flows(db=Depends(get_db), u=Depends(current_user)):
    out = []
    for f in db.scalars(select(Flow).order_by(Flow.id.desc())):
        rev = db.scalar(select(FlowRevision).where(FlowRevision.flow_id == f.id)
                        .order_by(FlowRevision.revision.desc()).limit(1))
        last = db.scalar(select(PipelineRun).where(PipelineRun.flow_id == f.id)
                         .order_by(PipelineRun.id.desc()).limit(1))
        out.append(flow_dict(f, revision=rev.revision if rev else None,
                             publication=_publication_summary(db, f.current_publication_id),
                             last_run=run_dict(last) if last else None))
    return out


@router.post('/flows', status_code=201)
def create(data: FlowInput, db=Depends(get_db), u=Depends(can_write)):
    flow = pipeline.create_flow(db, data.name, data.definition.model_dump(), data.note, u)
    record(db, u.email, 'flow.create', {'id': flow.id})
    db.commit()
    return flow_dict(flow, revision=1)


@router.get('/flows/{flow_id}')
def flow_detail(flow_id: int, db=Depends(get_db), u=Depends(current_user)):
    flow = _get_flow(db, flow_id)
    revisions = db.scalars(select(FlowRevision).where(FlowRevision.flow_id == flow.id)
                           .order_by(FlowRevision.revision.desc()))
    revisions = list(revisions)
    runs = db.scalars(select(PipelineRun).where(PipelineRun.flow_id == flow.id)
                      .order_by(PipelineRun.id.desc()).limit(20))
    return {**flow_dict(flow, revision=revisions[0].revision if revisions else None,
                        publication=_publication_summary(db, flow.current_publication_id)),
            'revisions': [revision_dict(r) for r in revisions],
            'runs': [run_dict(r) for r in runs]}


@router.post('/flows/{flow_id}/revisions', status_code=201)
def revise(flow_id: int, data: RevisionInput, db=Depends(get_db), u=Depends(can_write)):
    flow = _get_flow(db, flow_id)
    rev = pipeline.save_revision(db, flow, data.definition.model_dump(), data.note, u)
    record(db, u.email, 'flow.revise', {'flow': flow.id, 'revision': rev.revision})
    db.commit()
    return revision_dict(rev)


@router.post('/flows/{flow_id}/runs', status_code=201)
def start_run(flow_id: int, db=Depends(get_db), u=Depends(can_write)):
    flow = _get_flow(db, flow_id)
    run = pipeline.create_run(db, flow, u)
    record(db, u.email, 'pipeline.run.create', {'id': run.id, 'flow': flow.id})
    db.commit()
    pipeline.enqueue(run.id)
    return run_dict(run)


@router.get('/flows/{flow_id}/publications')
def publications(flow_id: int, db=Depends(get_db), u=Depends(current_user)):
    _get_flow(db, flow_id)
    return [publication_dict(p) for p in db.scalars(
        select(Publication).where(Publication.flow_id == flow_id)
        .order_by(Publication.id.desc()).limit(20))]


@router.get('/runs/{run_id}')
def run_detail(run_id: int, db=Depends(get_db), u=Depends(current_user)):
    run = db.get(PipelineRun, run_id)
    if not run:
        raise HTTPException(404, '运行不存在')
    steps = db.scalars(select(StepRun).where(StepRun.run_id == run_id).order_by(StepRun.id))
    flow = db.get(Flow, run.flow_id)
    published = flow and flow.current_publication_id and db.scalar(
        select(Publication.id).where(Publication.run_id == run_id)
        .limit(1)) == flow.current_publication_id
    return {**run_dict(run), 'published': bool(published),
            'steps': [step_run_dict(s) for s in steps]}


@router.post('/runs/{run_id}/retry')
def retry_run(run_id: int, db=Depends(get_db), u=Depends(can_write)):
    run = db.get(PipelineRun, run_id)
    if not run:
        raise HTTPException(404, '运行不存在')
    pipeline.retry(db, run, u)
    record(db, u.email, 'pipeline.run.retry', {'id': run.id})
    db.commit()
    pipeline.enqueue(run.id)
    return run_dict(run)


@router.get('/batches/{batch_id}')
def batch_detail(batch_id: int, db=Depends(get_db), u=Depends(current_user)):
    batch = db.get(Batch, batch_id)
    if not batch:
        raise HTTPException(404, '中间批次不存在')
    produced_by = db.get(PipelineRun, batch.run_id)
    consumers = db.scalars(select(StepRun).where(StepRun.run_id == batch.run_id))
    used_by = [s.step_key for s in consumers if batch.step_key in (s.parents or [])]
    return {**batch_dict(batch, with_records=True),
            'produced_by': {'run_id': batch.run_id, 'step_key': batch.step_key,
                            'run_status': produced_by.status if produced_by else None},
            'used_by': used_by}
