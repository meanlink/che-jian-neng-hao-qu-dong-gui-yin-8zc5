import {api} from './client.js';
export const datasets={list:()=>api('/datasets'),get:id=>api('/datasets/'+id),create:data=>api('/datasets',{method:'POST',body:JSON.stringify(data)})};
export const jobs={list:()=>api('/jobs'),get:id=>api('/jobs/'+id),create:data=>api('/jobs',{method:'POST',body:JSON.stringify(data)}),approve:id=>api('/jobs/'+id+'/approve',{method:'POST'})};
export const reports={list:()=>api('/reports'),create:data=>api('/reports',{method:'POST',body:JSON.stringify(data)}),toggle:id=>api('/reports/'+id+'/toggle',{method:'POST'}),deliveries:()=>api('/deliveries')};
export const pipeline={
 flows:()=>api('/pipeline/flows'),
 createFlow:data=>api('/pipeline/flows',{method:'POST',body:JSON.stringify(data)}),
 flow:id=>api('/pipeline/flows/'+id),
 saveRevision:(id,data)=>api('/pipeline/flows/'+id+'/revisions',{method:'POST',body:JSON.stringify(data)}),
 startRun:id=>api('/pipeline/flows/'+id+'/runs',{method:'POST'}),
 publications:id=>api('/pipeline/flows/'+id+'/publications'),
 run:id=>api('/pipeline/runs/'+id),
 retry:id=>api('/pipeline/runs/'+id+'/retry',{method:'POST'}),
 batch:id=>api('/pipeline/batches/'+id),
};
