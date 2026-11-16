"""可复用加工流程：版本化定义、内容寻址中间批次、围栏并发、原子发布。

加工步骤（数据接入 → 筛除停产日 / 合并月份 / 按日期补入气温）组成有向无环图；
运行启动时锁定流程版本与输入批次指纹，执行中的改版只影响下次运行。
"""
import math
import re
import time
from collections import defaultdict

from fastapi import HTTPException
from redis import Redis
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models import Dataset, Flow, FlowRevision, PipelineRun, StepRun, Batch, Publication
from src.services.audit import record
from src.utils.serialization import checksum

QUEUE = 'pipeline:queue'
DONE = ('completed', 'reused')
# 每种步骤允许的上游数量（下界, 上界）
KIND_PARENTS = {'source': (0, 0), 'filter_halt': (1, 1), 'merge': (2, 8), 'join_temp': (1, 1)}
FILTER_FIELDS = ('units', 'run_hours')
ON_MISSING = ('fail', 'drop')
EXIT_FIELDS = ('units', 'run_hours', 'temp_c', 'idle_hours', 'kwh')
KEY_RE = re.compile(r'^[a-z][a-z0-9_]{0,39}$')


# ---------- 定义校验：循环依赖在此阻止 ----------

def _normalize_params(db, kind, raw):
    p = raw or {}
    if kind == 'source':
        try:
            dataset_id = int(p.get('dataset_id'))
        except (TypeError, ValueError):
            raise HTTPException(422, '数据接入步骤需要选择输入批次') from None
        if not db.get(Dataset, dataset_id):
            raise HTTPException(422, f'输入数据批次 {dataset_id} 不存在')
        return {'dataset_id': dataset_id}
    if kind == 'filter_halt':
        field = str(p.get('field', 'units'))
        if field not in FILTER_FIELDS:
            raise HTTPException(422, '筛除停产日仅支持按日产量或运行小时判定')
        try:
            threshold = float(p.get('threshold', 0))
        except (TypeError, ValueError):
            raise HTTPException(422, '停产阈值必须为数值') from None
        if not math.isfinite(threshold) or not 0 <= threshold <= 1_000_000:
            raise HTTPException(422, '停产阈值须介于 0 与 1,000,000')
        return {'field': field, 'threshold': threshold}
    if kind == 'join_temp':
        try:
            temp_dataset_id = int(p.get('temp_dataset_id'))
        except (TypeError, ValueError):
            raise HTTPException(422, '补入气温需要选择气温数据批次') from None
        if not db.get(Dataset, temp_dataset_id):
            raise HTTPException(422, f'气温数据批次 {temp_dataset_id} 不存在')
        on_missing = str(p.get('on_missing', 'fail'))
        if on_missing not in ON_MISSING:
            raise HTTPException(422, '缺测处理方式须为 fail 或 drop')
        return {'temp_dataset_id': temp_dataset_id, 'on_missing': on_missing}
    return {}


def validate_definition(db, raw_steps):
    """校验并规范化步骤定义；循环依赖、悬空依赖、缺出口一律拒绝。"""
    if not isinstance(raw_steps, list) or not raw_steps:
        raise HTTPException(422, '流程至少需要一个步骤')
    if len(raw_steps) > 40:
        raise HTTPException(422, '单条流程最多 40 个步骤')
    steps, seen = [], set()
    for raw in raw_steps:
        key = str((raw or {}).get('key', '')).strip()
        if not KEY_RE.match(key):
            raise HTTPException(422, f'步骤标识「{key}」需为小写字母开头的英文、数字或下划线')
        if key in seen:
            raise HTTPException(422, f'步骤标识 {key} 重复')
        seen.add(key)
        kind = str((raw or {}).get('kind', ''))
        if kind not in KIND_PARENTS:
            raise HTTPException(422, f'未知步骤类型 {kind}')
        name = str((raw or {}).get('name', '')).strip()[:160] or key
        parents = [str(p) for p in ((raw or {}).get('parents') or [])]
        lo, hi = KIND_PARENTS[kind]
        if not lo <= len(parents) <= hi:
            need = f'{lo}' if lo == hi else f'{lo}–{hi}'
            raise HTTPException(422, f'步骤「{name}」需要 {need} 个上游步骤')
        if len(set(parents)) != len(parents):
            raise HTTPException(422, f'步骤「{name}」的上游重复')
        steps.append({'key': key, 'kind': kind, 'name': name, 'parents': parents,
                      'params': _normalize_params(db, kind, (raw or {}).get('params')),
                      'is_exit': bool((raw or {}).get('is_exit'))})
    children, indegree = defaultdict(list), {}
    for s in steps:
        indegree[s['key']] = 0
    for s in steps:
        for p in s['parents']:
            if p == s['key']:
                raise HTTPException(422, f'步骤「{s["name"]}」不能依赖自身')
            if p not in seen:
                raise HTTPException(422, f'步骤「{s["name"]}」依赖了不存在的步骤 {p}')
            children[p].append(s['key'])
            indegree[s['key']] += 1
    queue, visited = [k for k, d in indegree.items() if d == 0], 0
    order = []
    while queue:
        node = queue.pop()
        visited += 1
        order.append(node)
        for child in children[node]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)
    if visited != len(steps):
        raise HTTPException(422, '流程存在循环依赖，已阻止保存与运行')
    if not any(s['is_exit'] for s in steps):
        raise HTTPException(422, '至少需要一个出口步骤，发布时按出口切换批次')
    by_key = {s['key']: s for s in steps}
    return [by_key[k] for k in order]


# ---------- 气温工具：一日多值即阻止 ----------

def temp_map(records):
    """构造 日期→气温 映射；同一日期出现多个不同气温值时抛错。"""
    mapping = {}
    for row in records:
        day = str(row.get('day', '')).strip()
        value = row.get('temp_c')
        if not day or value is None:
            continue
        try:
            value = float(value)
        except (TypeError, ValueError):
            raise ValueError(f'气温记录 {day} 不是数值') from None
        if not math.isfinite(value):
            raise ValueError(f'气温记录 {day} 不是有限数值')
        if day in mapping and abs(mapping[day] - value) > 1e-9:
            raise ValueError(f'气温记录 {day} 一日多值（{mapping[day]} 与 {value}），已阻止运行')
        mapping[day] = value
    return mapping


# ---------- 流程与版本 ----------

def _latest_revision(db, flow_id):
    return db.scalar(select(FlowRevision).where(FlowRevision.flow_id == flow_id)
                     .order_by(FlowRevision.revision.desc()).limit(1))


def create_flow(db, name, definition, note, user):
    name = name.strip()
    if len(name) < 2:
        raise HTTPException(422, '流程名称去除空格后至少需要两个字符')
    if db.scalar(select(Flow).where(Flow.name == name)):
        raise HTTPException(409, '该流程名已存在，请修改名称')
    steps = validate_definition(db, (definition or {}).get('steps'))
    flow = Flow(name=name, created_by=user.id, created_at=time.time())
    db.add(flow)
    db.flush()
    db.add(FlowRevision(flow_id=flow.id, revision=1, definition={'steps': steps},
                        note=(note or '')[:400], created_by=user.id, created_at=time.time()))
    db.flush()
    return flow


def save_revision(db, flow, definition, note, user):
    """改版即产生新版本；正在执行的运行仍绑定旧版本，留待下次。"""
    steps = validate_definition(db, (definition or {}).get('steps'))
    latest = _latest_revision(db, flow.id)
    rev = FlowRevision(flow_id=flow.id, revision=(latest.revision if latest else 0) + 1,
                       definition={'steps': steps}, note=(note or '')[:400],
                       created_by=user.id, created_at=time.time())
    db.add(rev)
    db.flush()
    return rev


# ---------- 运行：锁定版本与输入 ----------

def _lock_inputs(db, steps):
    """解析全部输入批次并记录指纹；气温一日多值在此阻止运行。"""
    locked, temp_ids = {}, set()
    for s in steps:
        if s['kind'] == 'source':
            locked.setdefault(s['params']['dataset_id'], None)
        if s['kind'] == 'join_temp':
            temp_ids.add(s['params']['temp_dataset_id'])
            locked.setdefault(s['params']['temp_dataset_id'], None)
    for dataset_id in locked:
        d = db.get(Dataset, dataset_id)
        if not d:
            raise HTTPException(422, f'输入数据批次 {dataset_id} 不存在，已阻止运行')
        locked[dataset_id] = {'dataset_id': d.id, 'name': d.name, 'checksum': d.checksum,
                              'rows': len(d.records)}
        if dataset_id in temp_ids:
            try:
                temp_map(d.records)
            except ValueError as e:
                raise HTTPException(422, f'气温校验失败：{e}') from None
    return locked


def _fingerprints(steps, locked):
    """按拓扑序计算每步内容指纹：规则或上游产物变化即得到新指纹。"""
    fps = {}
    for s in steps:
        if s['kind'] == 'source':
            params = {'dataset': locked[s['params']['dataset_id']]['checksum']}
        elif s['kind'] == 'join_temp':
            params = {'temp': locked[s['params']['temp_dataset_id']]['checksum'],
                      'on_missing': s['params']['on_missing']}
        else:
            params = s['params']
        payload = {'kind': s['kind'], 'params': params,
                   'parents': sorted(fps[p] for p in s['parents'])}
        fps[s['key']] = checksum(payload)
    return fps


def create_run(db, flow, user):
    rev = _latest_revision(db, flow.id)
    if not rev:
        raise HTTPException(409, '流程还没有可用版本，请先保存定义')
    steps = validate_definition(db, rev.definition.get('steps'))
    locked = _lock_inputs(db, steps)
    fps = _fingerprints(steps, locked)
    run = PipelineRun(flow_id=flow.id, revision_id=rev.id, status='queued',
                      inputs=list(locked.values()), created_by=user.id, created_at=time.time())
    db.add(run)
    db.flush()
    for s in steps:
        db.add(StepRun(run_id=run.id, step_key=s['key'], name=s['name'], kind=s['kind'],
                       params=s['params'], parents=s['parents'], is_exit=s['is_exit'],
                       fingerprint=fps[s['key']], status='pending'))
    db.flush()
    return run


def enqueue(run_id):
    try:
        r = Redis.from_url(REDIS_URL)
        r.rpush(QUEUE, run_id)
        r.close()
    except Exception:
        pass  # SQL 中的 queued 状态是持久兜底，worker 周期扫描


# ---------- 步骤计算 ----------

def execute_step_records(kind, params, parent_rows, source_records, temp_records):
    if kind == 'source':
        return [dict(r) for r in source_records]
    if kind == 'filter_halt':
        field, threshold = params['field'], float(params['threshold'])
        out = []
        for row in parent_rows[0]:
            value = row.get(field)
            if value is None:
                raise ValueError(f'记录缺少字段 {field}，无法判定停产日')
            if float(value) > threshold:
                out.append(dict(row))
        return out
    if kind == 'merge':
        by_day = {}
        for rows in parent_rows:
            for row in rows:
                day = str(row.get('day', '')).strip()
                if not day:
                    raise ValueError('合并月份时发现缺少日期的记录')
                if day in by_day:
                    if checksum(by_day[day]) != checksum(row):
                        raise ValueError(f'合并月份时发现 {day} 存在冲突记录，请先核对来源')
                else:
                    by_day[day] = dict(row)
        return [by_day[day] for day in sorted(by_day)]
    if kind == 'join_temp':
        mapping = temp_map(temp_records)  # 一日多值在此再次拦截
        on_missing = params.get('on_missing', 'fail')
        out = []
        for row in parent_rows[0]:
            day = str(row.get('day', '')).strip()
            if day in mapping:
                out.append({**row, 'temp_c': mapping[day]})
            elif on_missing == 'drop':
                continue
            else:
                raise ValueError(f'{day} 缺少气温记录，已阻止运行')
        return out
    raise ValueError(f'未知步骤类型 {kind}')


def validate_exit(records):
    """出口校验：供归因使用的批次必须字段齐全、数值有限、日期唯一。"""
    if not records or len(records) < 5:
        raise ValueError('出口批次少于 5 条记录，不足以供归因使用')
    seen = set()
    for index, row in enumerate(records):
        day = str(row.get('day', '')).strip()
        if not day:
            raise ValueError(f'第 {index + 1} 行缺少日期')
        if day in seen:
            raise ValueError(f'日期 {day} 重复，需先聚合为一条记录')
        seen.add(day)
        values = {}
        for field in EXIT_FIELDS:
            value = row.get(field)
            try:
                value = float(value)
            except (TypeError, ValueError):
                raise ValueError(f'{day} 的 {field} 缺失或非数值') from None
            if not math.isfinite(value):
                raise ValueError(f'{day} 的 {field} 含 NaN 或无穷')
            values[field] = value
        if not (0 <= values['units'] <= 10_000_000 and 0 <= values['run_hours'] <= 24
                and -100 <= values['temp_c'] <= 100 and 0 <= values['idle_hours'] <= 24
                and 0 <= values['kwh'] <= 1_000_000_000):
            raise ValueError(f'{day} 的数值超出业务范围（产量/工时/温度/能耗）')


# ---------- 执行引擎：围栏认领，仅接纳当前尝试的产物 ----------

def _get_or_create_batch(db, step, run_id, records):
    batch = db.scalar(select(Batch).where(Batch.fingerprint == step.fingerprint))
    if batch:
        return batch
    batch = Batch(fingerprint=step.fingerprint, kind=step.kind, records=records,
                  rows=len(records), checksum=checksum(records), run_id=run_id,
                  step_key=step.step_key, created_at=time.time())
    db.add(batch)
    try:
        db.commit()
    except IntegrityError:  # 并发执行者已写入同一指纹批次
        db.rollback()
        batch = db.scalar(select(Batch).where(Batch.fingerprint == step.fingerprint))
    return batch


def _finalize(db, run, steps, executor):
    """全部出口通过校验后，在同一事务内切换发布指针；失败保留旧版。"""
    exits = [s for s in steps.values() if s.is_exit]
    payload = []
    try:
        for s in exits:
            batch = db.get(Batch, s.batch_id)
            validate_exit(batch.records)
            payload.append({'step_key': s.step_key, 'name': s.name, 'batch_id': batch.id,
                            'rows': batch.rows, 'checksum': batch.checksum})
    except ValueError as e:
        db.execute(update(PipelineRun).where(PipelineRun.id == run.id,
                                             PipelineRun.status.in_(['queued', 'running']))
                   .values(status='failed', error=f'出口校验失败：{e}'[:990],
                           finished_at=time.time()))
        record(db, executor, 'pipeline.run.failed', {'id': run.id, 'error': str(e)[:200]})
        db.commit()
        return
    claimed = db.execute(update(PipelineRun).where(PipelineRun.id == run.id,
                                                   PipelineRun.status == 'running')
                         .values(status='completed', finished_at=time.time()))
    if claimed.rowcount == 0:
        return  # 其他执行者已收尾
    flow = db.get(Flow, run.flow_id)
    publication = Publication(flow_id=flow.id, run_id=run.id, exits=payload,
                              created_at=time.time())
    db.add(publication)
    db.flush()
    for item in payload:
        batch = db.get(Batch, item['batch_id'])
        name = f"{flow.name} · {item['name']} · 运行#{run.id}"
        if not db.scalar(select(Dataset).where(Dataset.name == name)):
            db.add(Dataset(name=name, source=f'pipeline:flow/{flow.id}/run/{run.id}',
                           records=batch.records, checksum=batch.checksum,
                           created_at=time.time()))
    flow.current_publication_id = publication.id
    record(db, executor, 'pipeline.published',
           {'flow': flow.id, 'run': run.id, 'publication': publication.id})
    db.commit()


def advance(run_id, executor):
    """推进一次运行直到无可执行步骤；中断恢复与重试均沿用已完成步骤。"""
    while True:
        with Session() as db:
            run = db.get(PipelineRun, run_id)
            if not run or run.status not in ('queued', 'running'):
                return
            if run.status == 'queued':
                run.status = 'running'
                run.started_at = time.time()
                db.commit()
            steps = {s.step_key: s for s in db.scalars(select(StepRun).where(StepRun.run_id == run_id))}
            if any(s.status == 'failed' for s in steps.values()):
                return  # 失败事务中已把运行置为 failed
            if all(s.status in DONE for s in steps.values()):
                _finalize(db, run, steps, executor)
                return
            runnable = [s for s in steps.values() if s.status == 'pending'
                        and all(steps[p].status in DONE for p in s.parents)]
            if not runnable:
                return  # 其余步骤正由其他执行者推进
            step = sorted(runnable, key=lambda s: s.id)[0]
            existing = db.scalar(select(Batch).where(Batch.fingerprint == step.fingerprint))
            if existing:  # 内容指纹命中：重试/无关分支直接复用，不重复生成
                step.status = 'reused'
                step.batch_id = existing.id
                step.finished_at = time.time()
                record(db, executor, 'pipeline.step.reused',
                       {'run': run_id, 'step': step.step_key, 'batch': existing.id})
                db.commit()
                continue
            attempt = step.attempt + 1  # 先取本次尝试号；同步更新会改写内存中的 attempt
            claimed = db.execute(update(StepRun).where(StepRun.id == step.id,
                                                       StepRun.status == 'pending')
                                 .values(status='running', attempt=attempt,
                                         claimed_by=executor, started_at=time.time(), error=''))
            db.commit()
            if claimed.rowcount == 0:
                continue  # 已被其他执行者认领
            parent_rows, source_records, temp_records = [], None, None
            for p in step.parents:
                parent_rows.append(db.get(Batch, steps[p].batch_id).records)
            if step.kind == 'source':
                source_records = db.get(Dataset, step.params['dataset_id']).records
            if step.kind == 'join_temp':
                temp_records = db.get(Dataset, step.params['temp_dataset_id']).records
        try:
            records = execute_step_records(step.kind, step.params, parent_rows,
                                           source_records, temp_records)
            if step.is_exit:
                validate_exit(records)
            error = None
        except Exception as e:  # 计算或校验失败：仅当前尝试可写入失败态
            records, error = None, str(e)
        with Session() as db:
            if error is None:
                batch = _get_or_create_batch(db, step, run_id, records)
                accepted = db.execute(update(StepRun).where(StepRun.id == step.id,
                                                            StepRun.attempt == attempt,
                                                            StepRun.status == 'running')
                                      .values(status='completed', batch_id=batch.id,
                                              finished_at=time.time()))
                if accepted.rowcount:
                    record(db, executor, 'pipeline.step.completed',
                           {'run': run_id, 'step': step.step_key, 'batch': batch.id})
            else:
                accepted = db.execute(update(StepRun).where(StepRun.id == step.id,
                                                            StepRun.attempt == attempt,
                                                            StepRun.status == 'running')
                                      .values(status='failed', error=error[:990],
                                              finished_at=time.time()))
                if accepted.rowcount:
                    db.execute(update(PipelineRun).where(PipelineRun.id == run_id,
                                                         PipelineRun.status.in_(['queued', 'running']))
                               .values(status='failed', error=error[:990],
                                       finished_at=time.time()))
                    record(db, executor, 'pipeline.step.failed',
                           {'run': run_id, 'step': step.step_key, 'error': error[:200]})
            db.commit()


def retry(db, run, user):
    if run.status != 'failed':
        raise HTTPException(409, '仅失败的运行可以重试')
    for s in db.scalars(select(StepRun).where(StepRun.run_id == run.id)):
        if s.status in ('failed', 'running'):
            # 失败或僵死的步骤回到待运行；围栏保证迟到产物不被接纳
            s.status = 'pending'
            s.error = ''
    run.status = 'queued'
    run.error = ''
    run.finished_at = None
    db.flush()
    return run


def recover(db):
    """worker 重启：中断的步骤回到待运行（attempt 保留，旧尝试被围栏拒绝）。"""
    for s in db.scalars(select(StepRun).where(StepRun.status == 'running')):
        s.status = 'pending'
        record(db, 'worker', 'pipeline.step.recovered', {'run': s.run_id, 'step': s.step_key})
    for r in db.scalars(select(PipelineRun).where(PipelineRun.status == 'running')):
        r.status = 'queued'
        record(db, 'worker', 'pipeline.run.recovered', {'id': r.id})
