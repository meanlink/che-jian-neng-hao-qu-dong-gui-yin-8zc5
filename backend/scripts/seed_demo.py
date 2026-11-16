import time, json
from sqlalchemy import select, func
from src.config.database import Session
from migrations.initial import migrate
from src.models import (User, Dataset, Job, Report, Delivery, Audit,
                        Flow, FlowRevision, PipelineRun, StepRun, Batch, Publication)
from src.services.business import metadata, seed_records, analyze
from src.services.reports import deliver
from src.services import pipeline
from src.utils.security import hash_password
from src.utils.serialization import checksum

HALT_DAYS = {'2026-05-11', '2026-05-25', '2026-06-08', '2026-06-21', '2026-07-05', '2026-07-18'}


def _seed_pipeline(db, admin):
    """归因前加工演示：三个月抄表 → 合并月份 → 筛除停产日 → 按日期补入气温 → 发布。"""
    name = '归因前加工 · 澄川冲压一线'
    if db.scalar(select(Flow).where(Flow.name == name)):
        return
    rows = seed_records()
    months = {'2026-05': [], '2026-06': [], '2026-07': []}
    temps = []
    for row in rows:
        month = row['day'][:7]
        if month not in months:
            continue
        temps.append({'day': row['day'], 'temp_c': row['temp_c']})
        item = {k: v for k, v in row.items() if k != 'temp_c'}
        if row['day'] in HALT_DAYS:  # 停产检修日：无产量无运行，仅待机耗电
            item.update({'units': 0.0, 'run_hours': 0.0, 'kwh': round(40 + row['idle_hours'] * 38, 2)})
        months[month].append(item)
    datasets = {}
    for month, records in months.items():
        dname = f'抄表_{month}.csv'
        d = db.scalar(select(Dataset).where(Dataset.name == dname))
        if not d:
            d = Dataset(name=dname, source=f'抄表导出 {month}', records=records,
                        checksum=checksum(records), created_at=time.time())
            db.add(d)
            db.flush()
        datasets[month] = d
    tname = '气温_2026-05至07.csv'
    temp_ds = db.scalar(select(Dataset).where(Dataset.name == tname))
    if not temp_ds:
        temp_ds = Dataset(name=tname, source='车间温度记录仪', records=temps,
                          checksum=checksum(temps), created_at=time.time())
        db.add(temp_ds)
        db.flush()
    flow = pipeline.create_flow(db, name, {'steps': [
        {'key': 'src_may', 'kind': 'source', 'name': '5月抄表',
         'params': {'dataset_id': datasets['2026-05'].id}, 'parents': []},
        {'key': 'src_jun', 'kind': 'source', 'name': '6月抄表',
         'params': {'dataset_id': datasets['2026-06'].id}, 'parents': []},
        {'key': 'src_jul', 'kind': 'source', 'name': '7月抄表',
         'params': {'dataset_id': datasets['2026-07'].id}, 'parents': []},
        {'key': 'merge', 'kind': 'merge', 'name': '合并月份',
         'params': {}, 'parents': ['src_may', 'src_jun', 'src_jul']},
        {'key': 'filter', 'kind': 'filter_halt', 'name': '筛除停产日',
         'params': {'field': 'units', 'threshold': 0}, 'parents': ['merge']},
        {'key': 'temp', 'kind': 'join_temp', 'name': '按日期补入气温',
         'params': {'temp_dataset_id': temp_ds.id, 'on_missing': 'fail'},
         'parents': ['filter'], 'is_exit': True},
    ]}, '初始版本：三个月抄表合并后筛除停产日，再按日期补入气温', admin)
    db.commit()
    run = pipeline.create_run(db, flow, admin)
    db.commit()
    pipeline.advance(run.id, 'seed')
    db.add(Audit(actor='seed', action='pipeline.demo', detail={'flow': flow.id, 'run': run.id}, at=time.time()))


def seed():
    migrate()
    with Session() as db:
        for role,name in [('admin','林知 · 业务负责人'),('analyst','陈舟 · 分析员'),('viewer','周禾 · 观察员')]:
            email=role+'@demo.local'
            if not db.scalar(select(User).where(User.email==email)):
                db.add(User(email=email,name=name,role=role,password=hash_password('Demo123!')))
        db.flush()
        m=metadata()
        d=db.scalar(select(Dataset).where(Dataset.name==m['dataset_name']))
        if not d:
            rows=seed_records()
            d=Dataset(name=m['dataset_name'],source=m['short']+'-202611.csv',records=rows,checksum=checksum(rows),created_at=time.time())
            db.add(d); db.flush()
        if not db.scalar(select(Job).where(Job.name=='基线复核 · '+m['name'])):
            params={p['key']:p['default'] for p in m.get('parameters',[])}
            result=analyze(d.records,params)
            json.dumps(result,ensure_ascii=False,allow_nan=False)
            admin=db.scalar(select(User).where(User.role=='admin'))
            j=Job(name='基线复核 · '+m['name'],dataset_id=d.id,params=params,status='completed',result=result,created_by=admin.id,created_at=time.time()-3600,finished_at=time.time()-3595,approved=True)
            db.add(j);db.flush()
            r=Report(name='每日业务复核简报',job_id=j.id,destination='业务复核组 · 本地收件箱',interval_seconds=86400,next_run=time.time()+86400,enabled=True)
            db.add(r);db.flush();deliver(db,r)
            db.add(Audit(actor='seed',action='demo.baseline',detail={'dataset':d.id},at=time.time()))
        db.commit()
        admin=db.scalar(select(User).where(User.role=='admin'))
        _seed_pipeline(db, admin)
        db.commit()
        counts={model.__tablename__:db.scalar(select(func.count()).select_from(model)) for model in [User,Dataset,Job,Report,Delivery,Audit,Flow,FlowRevision,PipelineRun,StepRun,Batch,Publication]}
        print(json.dumps(counts,ensure_ascii=False))
        return counts
if __name__=='__main__':seed()
