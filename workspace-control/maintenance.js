#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const { WorkspaceControlPlane } = require('./control-plane');

const WORKSPACE_ROOT = path.resolve(process.env.MCPX_WORKSPACE_ROOT || path.join(os.homedir(),'.local','share','mcpx-workspaces'));
const RECOVERY_DIR = path.resolve(process.env.MCPX_RECOVERY_DIR || path.join(__dirname,'..','recovery'));
const SERVER_VERSION = process.env.MCPX_SERVER_VERSION || '0.1.0';
const DRY_RUN = process.argv.includes('--dry-run');
const CLEAN_IDLE_MS = Math.max(60_000, Number(process.env.MCPX_CLEAN_IDLE_MS) || 24*60*60*1000);
const DIVERGENT_COLD_MS = Math.max(CLEAN_IDLE_MS, Number(process.env.MCPX_DIVERGENT_COLD_MS) || 7*24*60*60*1000);
const INTEGRATED_GRACE_MS = Math.max(60_000, Number(process.env.MCPX_INTEGRATED_GC_GRACE_MS) || 6*60*60*1000);
const NOW = Date.now();

function workspacePaths(id){const dir=path.join(WORKSPACE_ROOT,id);return{dir,work:path.join(dir,'work'),base:path.join(dir,'base'),manifest:path.join(dir,'manifest.json'),conflicts:path.join(dir,'conflicts'),integrations:path.join(dir,'integrations')}}
function readJson(file){try{return JSON.parse(fs.readFileSync(file,'utf8'))}catch(_){return null}}
function durableWriteJson(file,value){const dir=path.dirname(file);fs.mkdirSync(dir,{recursive:true,mode:0o700});const tmp=`${file}.tmp.${process.pid}.${crypto.randomUUID()}`;const fd=fs.openSync(tmp,fs.constants.O_WRONLY|fs.constants.O_CREAT|fs.constants.O_EXCL,0o600);try{fs.writeFileSync(fd,`${JSON.stringify(value,null,2)}\n`);fs.fsyncSync(fd)}finally{fs.closeSync(fd)}fs.renameSync(tmp,file);try{const dfd=fs.openSync(dir,fs.constants.O_RDONLY);try{fs.fsyncSync(dfd)}finally{fs.closeSync(dfd)}}catch(_){}}
function sha(p){const h=crypto.createHash('sha256');const fd=fs.openSync(p,'r');try{const b=Buffer.alloc(1024*1024);let n;while((n=fs.readSync(fd,b,0,b.length,null))>0)h.update(b.subarray(0,n))}finally{fs.closeSync(fd)}return h.digest('hex')}
function leaf(p){try{const st=fs.lstatSync(p);if(st.isSymbolicLink())return['symlink',fs.readlinkSync(p),null,st.mtimeMs];if(st.isDirectory())return['dir',null,null,st.mtimeMs];if(st.isFile())return['file',null,st.size,st.mtimeMs];return['other',null,null,st.mtimeMs]}catch(e){if(e.code==='ENOENT')return['absent',null,null,null];throw e}}
function diffTree(base,work,rel,out){const a=path.join(base,rel),b=path.join(work,rel),as=leaf(a),bs=leaf(b);if(as[0]!==bs[0]){out.add(rel);return}if(as[0]==='absent')return;if(as[0]==='symlink'){if(as[1]!==bs[1])out.add(rel);return}if(as[0]==='file'){if(as[2]===bs[2]&&as[3]===bs[3])return;if(sha(a)!==sha(b))out.add(rel);return}if(as[0]==='dir'){let an=[],bn=[];try{an=fs.readdirSync(a)}catch(_){}try{bn=fs.readdirSync(b)}catch(_){}for(const n of new Set([...an,...bn]))diffTree(base,work,path.join(rel,n),out)}}
async function workspaceChanged(manifest){const wp=workspacePaths(manifest.id),out=new Set();for(const rel of manifest.mutable_paths||[])diffTree(wp.base,wp.work,rel,out);return [...out].sort()}
function jobRecords(){const out=[];const d=path.join(RECOVERY_DIR,'jobs');let names=[];try{names=fs.readdirSync(d)}catch(_){return out}for(const n of names){if(!n.endsWith('.json'))continue;const j=readJson(path.join(d,n));if(j)out.push(j)}return out}
function processGroupAlive(pid){if(!pid)return null;try{process.kill(-Number(pid),0);return true}catch(e){if(e.code==='EPERM')return true;if(e.code==='ESRCH')return false;return null}}
const JOBS=jobRecords();
function liveJobsForWorkspace(id){const want=path.resolve(workspacePaths(id).work),rows=[];for(const j of JOBS){if(path.resolve(j.cwd||'')!==want||j.status!=='running')continue;const alive=processGroupAlive(j.pid);if(alive===false)continue;rows.push({job_id:j.job_id,operation_id:j.operation_id,status:j.status,pid:j.pid,alive})}return rows}

const control = new WorkspaceControlPlane({workspaceRoot:WORKSPACE_ROOT,recoveryDir:RECOVERY_DIR,serverVersion:SERVER_VERSION,workspacePaths,readJson,writeJson:durableWriteJson,workspaceChanged,runningJobsForWorkspace:liveJobsForWorkspace});
control.bootstrap();

function recordAge(rec){const t=Date.parse(rec.updated_at||rec.created_at||'');return Number.isFinite(t)?NOW-t:0}
async function withLocks(rec,fn){const releaseP=await control.acquireProjectLock(rec.project_path);let releaseW=null;try{releaseW=await control.acquireWorkspaceLock(rec.workspace_id);return await fn()}finally{if(releaseW)await releaseW();await releaseP()}}
function safeRename(src,dst){fs.mkdirSync(path.dirname(dst),{recursive:true,mode:0o700});if(fs.existsSync(dst))throw new Error(`destination exists: ${dst}`);fs.renameSync(src,dst)}
const result={at:new Date().toISOString(),dry_run:DRY_RUN,clean_idle_ms:CLEAN_IDLE_MS,divergent_cold_ms:DIVERGENT_COLD_MS,integrated_grace_ms:INTEGRATED_GRACE_MS,actions:[],skipped:[],errors:[]};
function act(kind,id,extra={}){result.actions.push({kind,workspace_id:id,...extra})}
function skip(reason,id,extra={}){result.skipped.push({reason,workspace_id:id,...extra})}

async function recoverPendingRecord(rec){
  if(!rec?.workspace_id||!rec?.project_path)return;
  const id=rec.workspace_id,wp=workspacePaths(id);
  await withLocks(rec,async()=>{
    const cur=control.loadWorkspace(id);if(!cur)return;const activeExists=fs.existsSync(wp.dir);
    if(cur.state==='retired'&&cur.pending_retirement_path){const dst=cur.pending_retirement_path;if(activeExists&&!fs.existsSync(dst)){if(!DRY_RUN)safeRename(wp.dir,dst);act('recover_retirement_move',id,{dst})}if(!DRY_RUN&&fs.existsSync(dst))control.finalizePhysicalState(id,{physical_present:false,physical_workspace_path:null,retired_storage_path:dst,pending_retirement_path:null});return}
    if(cur.state==='suspended'&&cur.resume_pending){const src=cur.suspension?.storage_path||cur.suspended_storage_path;const storageExists=!!src&&fs.existsSync(src);if(activeExists&&storageExists)throw new Error(`resume recovery ambiguous: both open and suspended storage exist for ${id}`);if(!activeExists&&!storageExists)throw new Error(`resume recovery missing both open and suspended storage for ${id}`);if(!activeExists&&storageExists){if(!DRY_RUN)safeRename(src,wp.dir);act('recover_resume_move',id,{src})}if(!DRY_RUN){control.markResumed(id);control.finalizePhysicalState(id,{physical_present:true,physical_workspace_path:wp.dir,suspension:null,resume_pending:null})}act('recover_interrupted_resume',id);return}
    if(cur.state==='suspended'&&cur.physical_present===true&&cur.suspension?.storage_path){const dst=cur.suspension.storage_path;if(activeExists&&!fs.existsSync(dst)){if(!DRY_RUN)safeRename(wp.dir,dst);act('recover_suspend_move',id,{dst})}if(!DRY_RUN&&fs.existsSync(dst))control.finalizePhysicalState(id,{physical_present:false,physical_workspace_path:null});return}
    if(cur.payload_state==='garbage_collected'&&cur.physical_present===true&&activeExists){if(!DRY_RUN)fs.rmSync(wp.dir,{recursive:true,force:false});if(!DRY_RUN)control.finalizePhysicalState(id,{physical_present:false,physical_workspace_path:null,pending_gc:false});act('recover_payload_gc',id);return}
    if(cur.state==='retired'&&!cur.preserved_dirty&&cur.retired_storage_path&&fs.existsSync(cur.retired_storage_path)){if(!DRY_RUN)fs.rmSync(cur.retired_storage_path,{recursive:true,force:false});if(!DRY_RUN)control.finalizePhysicalState(id,{retired_storage_path:null,retired_payload_gced_at:new Date().toISOString()});act('gc_clean_retired_storage',id);return}
  });
}

async function recoverPending(){let files=[];try{files=fs.readdirSync(path.join(RECOVERY_DIR,'workspaces')).filter(x=>x.endsWith('.json'))}catch(_){return}for(const n of files){const rec=readJson(path.join(RECOVERY_DIR,'workspaces',n));try{await recoverPendingRecord(rec)}catch(e){result.errors.push({workspace_id:rec?.workspace_id||n,error:`pending recovery: ${e.stack||e}`})}}}

async function handleOrphan(id){const dir=workspacePaths(id).dir;if(liveJobsForWorkspace(id).length){skip('orphan_live_job',id);return}const dst=path.join(RECOVERY_DIR,'workspace-orphans',id);if(DRY_RUN){act('quarantine_orphan',id,{dst});return}safeRename(dir,dst);const p=path.join(RECOVERY_DIR,'workspaces',`${id}.json`);const old=readJson(p)||{};durableWriteJson(p,{...old,workspace_id:id,state:'orphan_quarantined',physical_present:false,orphan_storage_path:dst,quarantined_at:new Date().toISOString(),updated_at:new Date().toISOString()});act('quarantine_orphan',id,{dst})}

async function handleWorkspace(id){
  const rec=control.loadWorkspace(id),wp=workspacePaths(id),manifest=readJson(wp.manifest);if(!rec||!manifest)return;
  if(liveJobsForWorkspace(id).length){skip('live_job',id);return}
  await withLocks(rec,async()=>{
    const current=control.loadWorkspace(id),m=readJson(wp.manifest);if(!current||!m)return;
    const live=liveJobsForWorkspace(id);if(live.length){skip('live_job_after_lock',id,{jobs:live});return}
    const age=recordAge(current);
    if(current.state==='integrated'||m.status==='integrated'){
      if(age<INTEGRATED_GRACE_MS){skip('integrated_grace',id,{age_ms:age});return}
      if(DRY_RUN){act('gc_integrated_payload',id);return}
      control.markPayloadGarbageCollected(id,{gc_reason:'maintenance_integrated',physical_present:true,physical_workspace_path:wp.dir,pending_gc:true});fs.rmSync(wp.dir,{recursive:true,force:false});control.finalizePhysicalState(id,{physical_present:false,physical_workspace_path:null,pending_gc:false});act('gc_integrated_payload',id);return;
    }
    if(current.state!=='open'||m.status!=='open'){skip('non_open_state',id,{state:current.state,manifest_status:m.status});return}
    const changed=await workspaceChanged(m);
    if(!changed.length){
      if(age<CLEAN_IDLE_MS){skip('clean_grace',id,{age_ms:age});return}
      const storage=path.join(RECOVERY_DIR,'workspace-retired','physical',id);
      if(DRY_RUN){act('retire_clean_idle',id,{storage});return}
      control.markRetired(id,'maintenance_clean_idle',{state:'retired',physical_present:true,pending_retirement_path:storage,changed_paths:[],preserved_dirty:false});safeRename(wp.dir,storage);control.finalizePhysicalState(id,{physical_present:false,physical_workspace_path:null,retired_storage_path:storage,pending_retirement_path:null});fs.rmSync(storage,{recursive:true,force:false});control.finalizePhysicalState(id,{retired_storage_path:null,retired_payload_gced_at:new Date().toISOString()});act('retire_clean_idle',id);return;
    }
    if(age<DIVERGENT_COLD_MS){skip('divergent_open',id,{age_ms:age,changed_count:changed.length});return}
    const storage=path.join(RECOVERY_DIR,'workspace-suspended',id);
    if(DRY_RUN){act('suspend_divergent_idle',id,{changed_count:changed.length,storage});return}
    control.markSuspended(id,{storage_path:storage,changed_paths:changed,suspended_by:'maintenance',base_work_preserved:true,physical_present:true});safeRename(wp.dir,storage);control.finalizePhysicalState(id,{physical_present:false,physical_workspace_path:null});act('suspend_divergent_idle',id,{changed_count:changed.length,storage});
  });
}

(async()=>{
  await recoverPending();
  let names=[];try{names=fs.readdirSync(WORKSPACE_ROOT)}catch(e){console.error(e);process.exit(2)}
  for(const id of names.sort()){
    if(!/^[0-9a-f-]{36}$/i.test(id))continue;
    try{const manifest=readJson(workspacePaths(id).manifest);if(!manifest)await handleOrphan(id);else await handleWorkspace(id)}catch(e){result.errors.push({workspace_id:id,error:String(e.stack||e)})}
  }
  const report=path.join(RECOVERY_DIR,'workspace-maintenance-latest.json');durableWriteJson(report,result);console.log(JSON.stringify({...result,actions_count:result.actions.length,skipped_count:result.skipped.length,errors_count:result.errors.length},null,2));if(result.errors.length)process.exitCode=1;
})().catch(e=>{console.error(e);process.exit(2)});
