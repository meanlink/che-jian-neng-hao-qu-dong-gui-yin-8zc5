import {pipeline,datasets} from '../api/entities.js';
import {workspace} from '../stores/workspace.js';
import {session} from '../stores/session.js';
import {heading} from '../components/common/shell.js';
import {table} from '../components/common/table.js';
import {esc,date,notify} from '../utils/dom.js';
import {submit} from '../hooks/forms.js';
import {RunStatus,StepStatus,StepKind} from '../constants/enums.js';

let editor=null,poll=null;

const KINDS=Object.keys(StepKind);
const badge=s=>'<span class="status st-'+esc(s)+'">'+esc(StepStatus[s]||RunStatus[s]||s)+'</span>';
const canWrite=()=>session.user.role!=='viewer';

function template(){
 const ds=workspace.datasets,first=ds[0],second=ds[1]||ds[0];
 return {steps:[
  {key:'src_a',kind:'source',name:'上月抄表',params:{dataset_id:first.id},parents:[],is_exit:false},
  {key:'src_b',kind:'source',name:'本月抄表',params:{dataset_id:second.id},parents:[],is_exit:false},
  {key:'merge',kind:'merge',name:'合并月份',params:{},parents:['src_a','src_b'],is_exit:false},
  {key:'filter',kind:'filter_halt',name:'筛除停产日',params:{field:'units',threshold:0},parents:['merge'],is_exit:false},
  {key:'temp',kind:'join_temp',name:'按日期补入气温',params:{temp_dataset_id:first.id,on_missing:'fail'},parents:['filter'],is_exit:true},
 ]};
}

export async function pipelinePage(id){
 const ds=await datasets.list();workspace.datasets=ds;
 if(!id){
  const flows=await pipeline.flows();
  return heading('03 / PREP FLOW','加工流程','把筛除停产日、合并月份、按日期补入气温串成可复用流程。运行启动即锁定版本与输入，全部出口通过校验后一次性切换发布。')
   +'<div class="pipeline-grid"><section class="panel"><div class="section-title"><h2>流程清单</h2><span>'+flows.length+' 条</span></div>'
   +(flows.length?flows.map(f=>'<a class="batch-item" href="#/pipeline/'+f.id+'"><strong>'+esc(f.name)+'</strong><span>版本 v'+(f.revision??'—')+(f.last_run?' · 最近运行 '+esc(RunStatus[f.last_run.status]||f.last_run.status):'')+'</span>'+(f.publication?'<small>已发布 · 运行#'+f.publication.run_id+' · '+date(f.publication.created_at)+'</small>':'<small>尚未发布</small>')+'</a>').join(''):'<p class="empty">还没有加工流程。</p>')
   +'</section><section class="panel"><div class="section-title"><h2>新建流程</h2></div><form id="flow-form"><label>流程名称<input name="name" required minlength="2" placeholder="例如：归因前加工 · 冲压一线"></label><p class="hint">将按模板生成「数据接入 ×2 → 合并月份 → 筛除停产日 → 按日期补入气温（出口）」，保存后可在编辑器中调整依赖与参数。</p><button type="submit" class="primary" '+(!canWrite()||!ds.length?'disabled':'')+'>按模板创建流程</button>'+(!ds.length?'<p class="hint">请先在数据接入导入批次，再创建流程。</p>':'')+'</form></section></div>';
 }
 const flow=await pipeline.flow(id);
 const latest=flow.revisions[0];
 editor={steps:latest?latest.definition.steps.map(s=>({key:s.key,kind:s.kind,name:s.name,parents:[...s.parents],params:{...s.params},is_exit:!!s.is_exit})):[],note:''};
 const pubs=await pipeline.publications(id);
 return heading('03 / PREP FLOW',flow.name,'版本 v'+flow.revision+' · 运行锁定版本与输入指纹；上游规则更新后下游自动失效重算，无关分支复用中间批次。','<button class="button" id="run-flow" '+(!canWrite()?'disabled':'')+'>运行此流程 ↗</button>')
  +'<div class="pipeline-detail"><section class="panel"><div class="section-title"><h2>步骤与依赖</h2><span>保存即产生新版本，执行中的运行不受影响</span></div><label>载入历史版本<select id="rev-select">'+flow.revisions.map(r=>'<option value="'+r.id+'">v'+r.revision+' · '+date(r.created_at)+(r.note?' · '+esc(r.note):'')+'</option>').join('')+'</select></label><div id="step-editor">'+editorHtml()+'</div><div class="editor-actions"><button class="button small" id="add-step" '+(!canWrite()?'disabled':'')+'>＋ 添加步骤</button></div><label>版本说明<input id="rev-note" placeholder="本次修改了什么规则" maxlength="400"></label><button class="primary" id="save-rev" '+(!canWrite()?'disabled':'')+'>保存为新版本</button><p class="hint">循环依赖、悬空依赖或缺少出口会被拒绝；气温一日多值在运行前阻止。</p></section>'
  +'<section><section class="panel"><div class="section-title"><h2>当前发布</h2><span>供归因使用的批次</span></div>'+(flow.publication?pubHtml(flow.publication,true):'<p class="empty">尚未发布。全部出口通过校验后，运行完成时一次性切换。</p>')+'</section>'
  +'<section class="panel"><div class="section-title"><h2>运行记录</h2><span>'+flow.runs.length+' 次</span></div><div id="run-list">'+runListHtml(flow.runs,flow.publication)+'</div></section>'
  +'<div id="run-detail"></div><div id="batch-detail"></div>'
  +'<section class="panel"><div class="section-title"><h2>发布历史</h2><span>'+pubs.length+' 次</span></div>'+(pubs.length?pubs.map(p=>pubHtml(p,false)).join(''):'<p class="empty">暂无发布记录。</p>')+'</section></section></div>';
}

function pubHtml(p,current){
 return '<div class="pub"><div class="pub-head"><strong>运行#'+p.run_id+'</strong><span>'+date(p.created_at)+(current?' · 当前生效':'')+'</span></div>'+p.exits.map(e=>'<div class="pub-exit"><span>出口 · '+esc(e.name)+'</span><span>'+e.rows+' 行 · SHA256 '+esc(String(e.checksum).slice(0,16))+'…</span></div>').join('')+'<p class="hint">对应数据批次已写入「数据接入」，可直接在分析工作台选择用于归因。</p></div>';
}

function runListHtml(runs,publication){
 if(!runs.length)return '<p class="empty">还没有运行。点击右上角「运行此流程」。</p>';
 return runs.map(r=>'<article class="run-item" data-run="'+r.id+'"><div><strong>运行#'+r.id+'</strong> '+badge(r.status)+(publication&&publication.run_id===r.id?' <span class="status st-published">已发布</span>':'')+'<p>'+date(r.created_at)+' · 锁定 '+(r.inputs||[]).length+' 个输入批次</p>'+(r.error?'<p class="run-error">'+esc(r.error)+'</p>':'')+'</div><span class="text-button">来源 ↗</span></article>').join('');
}

function editorHtml(){
 return editor.steps.map((s,i)=>{
  const others=editor.steps.filter(o=>o.key!==s.key);
  let params='';
  if(s.kind==='source')params='<label>输入批次<select data-step="'+i+'" data-param="dataset_id">'+workspace.datasets.map(d=>'<option value="'+d.id+'" '+(Number(s.params.dataset_id)===d.id?'selected':'')+'>'+esc(d.name)+' · '+d.rows+' 条</option>').join('')+'</select></label>';
  if(s.kind==='filter_halt')params='<div class="parameter-grid"><label>判定字段<select data-step="'+i+'" data-param="field"><option value="units" '+(s.params.field==='units'?'selected':'')+'>日产量</option><option value="run_hours" '+(s.params.field==='run_hours'?'selected':'')+'>运行小时</option></select></label><label>停产阈值（≤ 即剔除）<input type="number" step="any" min="0" data-step="'+i+'" data-param="threshold" value="'+esc(s.params.threshold??0)+'"></label></div>';
  if(s.kind==='join_temp')params='<div class="parameter-grid"><label>气温批次<select data-step="'+i+'" data-param="temp_dataset_id">'+workspace.datasets.map(d=>'<option value="'+d.id+'" '+(Number(s.params.temp_dataset_id)===d.id?'selected':'')+'>'+esc(d.name)+' · '+d.rows+' 条</option>').join('')+'</select></label><label>缺测处理<select data-step="'+i+'" data-param="on_missing"><option value="fail" '+(s.params.on_missing!=='drop'?'selected':'')+'>报错阻止</option><option value="drop" '+(s.params.on_missing==='drop'?'selected':'')+'>丢弃该行</option></select></label></div>';
  if(s.kind==='merge')params='<p class="hint">按日期合并全部上游；同一日期出现冲突记录时阻止运行。</p>';
  return '<div class="step-card" data-card="'+i+'"><div class="step-head"><span class="step-order">'+String(i+1).padStart(2,'0')+'</span><input class="step-name" data-step="'+i+'" data-field="name" value="'+esc(s.name)+'" placeholder="步骤名称"><select class="step-kind" data-step="'+i+'" data-field="kind">'+KINDS.map(k=>'<option value="'+k+'" '+(s.kind===k?'selected':'')+'>'+StepKind[k]+'</option>').join('')+'</select><label class="exit-flag"><input type="checkbox" data-step="'+i+'" data-field="is_exit" '+(s.is_exit?'checked':'')+'> 出口</label><button class="text-button" data-remove="'+i+'">移除</button></div><div class="step-body"><div class="step-meta"><label>标识<input data-step="'+i+'" data-field="key" value="'+esc(s.key)+'" placeholder="小写英文标识"></label>'+params+'</div>'+(others.length?'<div class="dep-list"><span>上游依赖：</span>'+others.map(o=>'<label class="dep"><input type="checkbox" data-step="'+i+'" data-parent="'+esc(o.key)+'" '+(s.parents.includes(o.key)?'checked':'')+'> '+esc(o.name)+'（'+esc(o.key)+'）</label>').join('')+'</div>':'<p class="hint">首个步骤无需上游；数据接入步骤不允许有上游。</p>')+'</div></div>';
 }).join('');
}

function readEditor(){
 document.querySelectorAll('[data-field]').forEach(el=>{
  const s=editor.steps[Number(el.dataset.step)];if(!s)return;
  const f=el.dataset.field;
  if(f==='is_exit')s.is_exit=el.checked;
  else if(f==='kind')s.kind=el.value;
  else s[f]=el.value.trim?el.value.trim():el.value;
 });
 document.querySelectorAll('[data-param]').forEach(el=>{
  const s=editor.steps[Number(el.dataset.step)];if(!s)return;
  const p=el.dataset.param;
  s.params[p]=(p==='dataset_id'||p==='temp_dataset_id')?Number(el.value):(p==='threshold'?Number(el.value):el.value);
 });
 document.querySelectorAll('[data-parent]').forEach(el=>{
  const s=editor.steps[Number(el.dataset.step)];if(!s)return;
  const key=el.dataset.parent,has=s.parents.includes(key);
  if(el.checked&&!has)s.parents.push(key);
  if(!el.checked&&has)s.parents=s.parents.filter(p=>p!==key);
 });
 editor.note=(document.querySelector('#rev-note')||{}).value||'';
}

function rerenderEditor(read=true){
 if(read)readEditor();
 const box=document.querySelector('#step-editor');
 box.innerHTML=editorHtml();
 bindEditorInner();
}

function bindEditorInner(){
 document.querySelectorAll('[data-field]').forEach(el=>el.addEventListener('change',()=>{const structural=el.dataset.field==='kind';readEditor();if(structural)rerenderEditor();}));
 document.querySelectorAll('[data-parent]').forEach(el=>el.addEventListener('change',readEditor));
 document.querySelectorAll('[data-param]').forEach(el=>el.addEventListener('change',readEditor));
 document.querySelectorAll('[data-remove]').forEach(b=>b.onclick=()=>{readEditor();editor.steps.splice(Number(b.dataset.remove),1);rerenderEditor();});
}

async function loadRun(runId){
 const box=document.querySelector('#run-detail');if(!box)return;
 try{
  const r=await pipeline.run(runId);
  box.innerHTML='<section class="panel"><div class="section-title"><h2>运行#'+r.id+' 逐步来源</h2><span>'+badge(r.status)+(r.published?' <span class="status st-published">已发布</span>':'')+'</span></div>'
   +'<p class="hint">锁定输入：'+r.inputs.map(i=>esc(i.name)+'（SHA256 '+esc(i.checksum.slice(0,10))+'…）').join('、')+'</p>'
   +'<div class="table-wrap"><table><thead><tr><th>步骤</th><th>类型</th><th>状态</th><th>尝试</th><th>执行者</th><th>批次指纹</th><th>上游</th><th>起止</th><th>错误</th></tr></thead><tbody>'
   +r.steps.map(s=>'<tr><td>'+esc(s.name)+(s.is_exit?' <span class="exit-tag">出口</span>':'')+'</td><td>'+esc(StepKind[s.kind]||s.kind)+'</td><td>'+badge(s.status)+'</td><td>'+s.attempt+'</td><td>'+esc(s.claimed_by||'—')+'</td><td>'+(s.batch_id?'<button class="text-button" data-batch="'+s.batch_id+'">'+esc(s.fingerprint)+'…</button>':'—')+'</td><td>'+esc((s.parents||[]).join(', ')||'—')+'</td><td>'+(s.started_at?date(s.started_at):'—')+(s.finished_at?'<br>'+date(s.finished_at):'')+'</td><td class="run-error">'+esc(s.error||'')+'</td></tr>').join('')
   +'</tbody></table></div>'
   +(r.status==='failed'&&canWrite()?'<button class="button small" id="retry-run">重试（沿用已完成步骤）↻</button>':'')
   +(r.error?'<p class="warning">'+esc(r.error)+'</p>':'')+'</section>';
  box.querySelectorAll('[data-batch]').forEach(b=>b.onclick=()=>loadBatch(b.dataset.batch));
  const retry=box.querySelector('#retry-run');
  if(retry)retry.onclick=async()=>{try{await pipeline.retry(runId);notify('已重新排队，沿用已完成步骤');await loadRun(runId);}catch(e){notify(e.message,true);}};
  if(r.status==='queued'||r.status==='running'){
   clearInterval(poll);poll=setInterval(async()=>{
    if(!document.querySelector('#run-detail')){clearInterval(poll);return;}
    const fresh=await pipeline.run(runId).catch(()=>null);if(!fresh)return;
    await loadRun(runId);
    if(fresh.status!=='queued'&&fresh.status!=='running'){
     clearInterval(poll);
     const item=document.querySelector('[data-run="'+runId+'"] .status');
     if(item){item.className='status st-'+fresh.status;item.textContent=RunStatus[fresh.status]||fresh.status;}
     notify('运行#'+runId+' '+(RunStatus[fresh.status]||fresh.status),fresh.status==='failed');
    }
   },2500);
  }
 }catch(e){notify(e.message,true);}
}

async function loadBatch(batchId){
 const box=document.querySelector('#batch-detail');if(!box)return;
 try{
  const b=await pipeline.batch(batchId);
  const cols=Object.keys(b.records[0]||{}).map(k=>({key:k,label:k}));
  box.innerHTML='<section class="panel"><div class="section-title"><h2>中间批次 #'+b.id+'</h2><span>'+b.rows+' 行 · '+esc(StepKind[b.kind]||b.kind)+'</span></div>'
   +'<p class="hint">指纹 '+esc(b.fingerprint.slice(0,24))+'… · 内容 SHA256 '+esc(b.checksum.slice(0,24))+'…<br>由运行#'+b.run_id+' 的步骤「'+esc(b.step_key)+'」生成于 '+date(b.created_at)+(b.used_by.length?'；被本运行的 '+esc(b.used_by.join('、'))+' 消费':'')+'。相同指纹的批次全局唯一，重试与无关分支直接复用。</p>'
   +'<div id="batch-preview">'+table(cols,b.records.slice(0,20),'空批次')+'</div></section>';
  box.scrollIntoView({behavior:'smooth',block:'nearest'});
 }catch(e){notify(e.message,true);}
}

export function bindPipeline(refresh,id){
 clearInterval(poll);
 if(!id){
  const form=document.querySelector('#flow-form');
  if(form)submit(form,async f=>{
   const def=template();
   const flow=await pipeline.createFlow({name:f.get('name'),definition:def,note:'模板创建'});
   notify('流程已创建，请在编辑器中核对依赖与参数');
   location.hash='#/pipeline/'+flow.id;
  });
  return;
 }
 bindEditorInner();
 document.querySelector('#add-step').onclick=()=>{readEditor();const n=editor.steps.length+1;editor.steps.push({key:'step_'+n,kind:'filter_halt',name:'新步骤',params:{field:'units',threshold:0},parents:[],is_exit:false});rerenderEditor();};
 document.querySelector('#save-rev').onclick=async()=>{try{readEditor();const rev=await pipeline.saveRevision(id,{definition:{steps:editor.steps},note:editor.note});notify('已保存为版本 v'+rev.revision+'，执行中的运行不受影响');await refresh();}catch(e){notify(e.message,true);}};
 document.querySelector('#rev-select').onchange=async e=>{try{const flow=await pipeline.flow(id);const rev=flow.revisions.find(r=>r.id===Number(e.target.value));if(rev){editor={steps:rev.definition.steps.map(s=>({...s,params:{...s.params},parents:[...s.parents]})),note:editor.note};rerenderEditor(false);notify('已载入版本 v'+rev.revision+'，保存将产生新版本');}}catch(err){notify(err.message,true);}};
 document.querySelector('#run-flow').onclick=async()=>{try{const r=await pipeline.startRun(id);notify('运行#'+r.id+' 已排队，启动时锁定版本与输入');await refresh();await loadRun(r.id);}catch(e){notify(e.message,true);}};
 document.querySelectorAll('[data-run]').forEach(el=>el.onclick=()=>loadRun(el.dataset.run));
}
