import hashlib, json
def checksum(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()).hexdigest()
def dataset_dict(d):
    return {'id':d.id, 'name':d.name, 'source':d.source, 'rows':len(d.records), 'checksum':d.checksum, 'created_at':d.created_at}
def job_dict(j):
    return {k:getattr(j,k) for k in ['id','name','dataset_id','params','status','result','error','approved','created_at','finished_at']}
def report_dict(r):
    return {k:getattr(r,k) for k in ['id','name','job_id','destination','interval_seconds','next_run','enabled']}
def delivery_dict(d):
    return {k:getattr(d,k) for k in ['id','report_id','created_at','checksum','filename','destination']}
def flow_dict(f,revision=None,publication=None,last_run=None):
    return {'id':f.id,'name':f.name,'created_at':f.created_at,'revision':revision,'publication':publication,'last_run':last_run}
def revision_dict(r):
    return {'id':r.id,'flow_id':r.flow_id,'revision':r.revision,'definition':r.definition,'note':r.note,'created_at':r.created_at}
def run_dict(r):
    return {k:getattr(r,k) for k in ['id','flow_id','revision_id','status','inputs','error','created_at','started_at','finished_at']}
def step_run_dict(s):
    return {k:getattr(s,k) for k in ['id','run_id','step_key','name','kind','params','parents','is_exit','status','attempt','claimed_by','batch_id','error','started_at','finished_at']} | {'fingerprint':s.fingerprint[:16]}
def batch_dict(b,with_records=False):
    d={k:getattr(b,k) for k in ['id','fingerprint','kind','rows','checksum','run_id','step_key','created_at']}
    if with_records:d['records']=b.records
    return d
def publication_dict(p):
    return {k:getattr(p,k) for k in ['id','flow_id','run_id','exits','created_at']}
