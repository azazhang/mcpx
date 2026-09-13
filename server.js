#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const os = require('os');
const { spawn, spawnSync } = require('child_process');
const crypto = require('crypto');
const http = require('http');
const { Readable } = require('stream');
const { finished } = require('stream/promises');
const readline = require('readline');
const { WorkspaceControlPlane, WorkspaceControlError } = require('./workspace-control/control-plane');
const {
  Server,
  createMcpHandler,
  WebStandardStreamableHTTPServerTransport,
  isLegacyRequest,
  isInitializeRequest,
} = require('@modelcontextprotocol/server');

const DEFAULT_BASE_DIR = path.resolve(process.env.MCPX_BASE_DIR || process.env.BASE_DIR || process.cwd());

const MCPX_SERVER_VERSION = '0.1.0';
const MCPX_BUILD_ID = process.env.MCPX_BUILD_ID || 'public-0.1.0';
const MCPX_SERVER_INSTANCE_ID = crypto.randomUUID();
const RECEIPT_DIR = process.env.MCPX_RECEIPT_DIR || path.join(__dirname, 'receipts');
const RECEIPT_LOG = process.env.MCPX_RESULT_RECEIPT_LOG || path.join(RECEIPT_DIR, 'tools-call-receipts.jsonl');
const RECEIPT_MAX_BYTES = Math.max(1024 * 1024, Number(process.env.MCPX_RECEIPT_MAX_BYTES) || 8 * 1024 * 1024);
fs.mkdirSync(RECEIPT_DIR, { recursive: true, mode: 0o700 });
try { fs.chmodSync(RECEIPT_DIR, 0o700); } catch (_) {}

const RESULT_DIR = process.env.MCPX_RESULT_DIR || path.join(__dirname, 'results');
const TRANSPORT_POST_LIMIT_BYTES = Math.max(0, Number(process.env.MCPX_TRANSPORT_POST_LIMIT_BYTES) || 0);
const TRANSPORT_ENVELOPE_BYTES = Math.max(0, Number(process.env.MCPX_TRANSPORT_ENVELOPE_BYTES) || 0);
const TRANSPORT_LIMIT_ENABLED = TRANSPORT_POST_LIMIT_BYTES > 0;
const SEMANTIC_RESPONSE_LIMIT_BYTES = Math.max(8 * 1024, Number(process.env.MCPX_SEMANTIC_RESPONSE_LIMIT_BYTES) || 32 * 1024);
const MODEL_RESPONSE_LIMIT_BYTES = Math.max(32 * 1024, Number(process.env.MCPX_MODEL_RESPONSE_LIMIT_BYTES) || 256 * 1024);
const MODEL_TEXT_PAYLOAD_BYTES = Math.max(8 * 1024, Number(process.env.MCPX_MODEL_TEXT_PAYLOAD_BYTES) || 64 * 1024);
fs.mkdirSync(RESULT_DIR, { recursive: true, mode: 0o700 });
try { fs.chmodSync(RESULT_DIR, 0o700); } catch (_) {}

function envFlag(name, fallback=false) {
  const raw = process.env[name];
  if (raw == null || raw === '') return fallback;
  return /^(1|true|yes|on)$/i.test(String(raw));
}
function findExecutable(name) {
  for (const dir of String(process.env.PATH || '').split(path.delimiter)) {
    if (!dir) continue;
    const candidate = path.join(dir, name);
    try { fs.accessSync(candidate, fs.constants.X_OK); return candidate; } catch (_) {}
  }
  return null;
}
const COMMAND_EXECUTION_ENABLED = envFlag('MCPX_ENABLE_COMMAND_EXECUTION', false);
const WORKSPACE_SANDBOX = String(process.env.MCPX_WORKSPACE_SANDBOX || 'none').toLowerCase();
const CODEX_BIN = process.env.MCPX_CODEX_BIN || findExecutable('codex');
const CODEX_PROFILE = process.env.MCPX_CODEX_PROFILE || 'mcpx_agent';
const SHELL_BIN = process.env.MCPX_SHELL || process.env.SHELL || '/bin/sh';
const PDFINFO_BIN = process.env.MCPX_PDFINFO_BIN || findExecutable('pdfinfo');
const PDFTOTEXT_BIN = process.env.MCPX_PDFTOTEXT_BIN || findExecutable('pdftotext');


const FILETXN_BIN = process.env.MCPX_FILETXN_BIN || path.join(__dirname, 'bin', 'mcpx-filetxn-atomic');
const FILETXN_DIR = process.env.MCPX_FILETXN_DIR || path.join(__dirname, 'file-transactions');
fs.mkdirSync(FILETXN_DIR, { recursive: true, mode: 0o700 });
try { fs.chmodSync(FILETXN_DIR, 0o700); } catch (_) {}
const fileMutationLocks = new Map();

function durableWriteJson(file, value) {
  const dir = path.dirname(file);
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const tmp = `${file}.tmp.${process.pid}.${crypto.randomUUID()}`;
  const fd = fs.openSync(tmp, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL, 0o600);
  try {
    fs.writeFileSync(fd, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
    fs.fsyncSync(fd);
  } finally { fs.closeSync(fd); }
  fs.renameSync(tmp, file);
  try {
    const dfd = fs.openSync(dir, fs.constants.O_RDONLY);
    try { fs.fsyncSync(dfd); } finally { fs.closeSync(dfd); }
  } catch (_) {}
}

async function withFileMutationLock(target, fn) {
  const key = path.normalize(target);
  const prior = fileMutationLocks.get(key) || Promise.resolve();
  let release;
  const gate = new Promise((r) => { release = r; });
  const tail = prior.then(() => gate);
  fileMutationLocks.set(key, tail);
  await prior;
  try { return await fn(); }
  finally { release(); if (fileMutationLocks.get(key) === tail) fileMutationLocks.delete(key); }
}

function txnJournalPath(id) { return path.join(FILETXN_DIR, `${id}.json`); }

async function createDurableTemp(target, content) {
  const dir = path.dirname(target);
  const base = path.basename(target);
  const tmp = path.join(dir, `.${base}.mcpx-tmp-${process.pid}-${crypto.randomUUID()}`);
  const fh = await fs.promises.open(tmp, 'wx', 0o600);
  try {
    await fh.writeFile(content, 'utf8');
    await fh.sync();
  } finally { await fh.close(); }
  return tmp;
}

async function fsyncDirectory(dir) {
  try { const fh = await fs.promises.open(dir, 'r'); try { await fh.sync(); } finally { await fh.close(); } } catch (_) {}
}

function runFileTxnHelper(op, target, expectedSha, replacement) {
  return new Promise((resolve, reject) => {
    const argv = [op, target, expectedSha || '-'];
    if (replacement) argv.push(replacement);
    const child = spawn(FILETXN_BIN, argv, { stdio: ['ignore', 'pipe', 'pipe'], env: process.env });
    const out=[]; const err=[]; let outBytes=0; let errBytes=0;
    const timer=setTimeout(() => { try { child.kill('SIGKILL'); } catch (_) {} }, 30000); timer.unref();
    child.stdout.on('data',(b)=>{ outBytes+=b.length; if(outBytes<=1024*1024) out.push(b); });
    child.stderr.on('data',(b)=>{ errBytes+=b.length; if(errBytes<=1024*1024) err.push(b); });
    child.on('error',(e)=>{ clearTimeout(timer); reject(e); });
    child.on('exit',(code,signal)=>{
      clearTimeout(timer);
      const stdout=Buffer.concat(out).toString('utf8').trim(); const stderr=Buffer.concat(err).toString('utf8').trim();
      if(code!==0) return reject(new Error(`mcpx-filetxn failed code=${code} signal=${signal || ''}: ${stderr || stdout}`));
      try { resolve(JSON.parse(stdout)); } catch (_) { reject(new Error(`mcpx-filetxn returned invalid JSON: ${stdout.slice(0,2000)}`)); }
    });
  });
}

async function targetRegularFileInfo(target) {
  try {
    const st = await fs.promises.lstat(target);
    if (st.isSymbolicLink()) return { exists:true, symlink:true, stat:st };
    if (!st.isFile()) return { exists:true, symlink:false, stat:st, regular:false };
    return { exists:true, symlink:false, regular:true, stat:st, sha256:await sha256File(target) };
  } catch (err) {
    if (err.code === 'ENOENT') return { exists:false };
    throw err;
  }
}

async function reconcileFileTxn(record, { cleanup = true } = {}) {
  let targetExists=false, targetSha=null;
  try {
    const st=await fs.promises.lstat(record.target);
    targetExists=true;
    if(st.isFile() && !st.isSymbolicLink()) targetSha=await sha256File(record.target);
  } catch (err) { if(err.code!=='ENOENT') throw err; }
  const tempExists=record.temp ? fs.existsSync(record.temp) : false;
  let state='not_completed';
  if(record.op==='delete') state = targetExists ? 'not_completed' : 'completed';
  else if(targetExists && targetSha===record.new_sha256) state='completed';
  else if(record.op==='create' && targetExists) state='conflict_or_external_change';
  if(cleanup) {
    if(tempExists) { try { await fs.promises.unlink(record.temp); } catch (_) {} }
    try { await fs.promises.unlink(txnJournalPath(record.id)); } catch (_) {}
    await fsyncDirectory(path.dirname(record.target));
  }
  return { state, target_exists:targetExists, target_sha256:targetSha, temp_exists:tempExists };
}

async function recoverFileTransactions() {
  let names=[]; try { names=await fs.promises.readdir(FILETXN_DIR); } catch (_) { return; }
  for(const name of names) {
    if(!name.endsWith('.json')) continue;
    const file=path.join(FILETXN_DIR,name); const rec=readJson(file);
    if(!rec || !rec.id || !rec.target) { try { await fs.promises.unlink(file); } catch (_) {} continue; }
    try {
      const result=await reconcileFileTxn(rec,{cleanup:true});
      console.error(`[filetxn recovery] id=${rec.id} op=${rec.op} state=${result.state}`);
    } catch(err) { console.error(`[filetxn recovery] id=${rec.id} failed: ${err.message}`); }
  }
}

async function performTransactionalWrite(target, content, options = {}) {
  return withFileMutationLock(target, async () => {
    const mode=options.mode || 'auto';
    const before=await targetRegularFileInfo(target);
    if(before.symlink) return { status:'error', reason:'symlink', message:`Refusing to mutate symbolic link: ${target}` };
    if(before.exists && !before.regular) return { status:'error', reason:'not_regular_file', message:`Refusing to replace non-regular file: ${target}` };
    let op;
    if(mode==='create') op='create';
    else if(mode==='replace') op='replace';
    else if(mode==='upsert' || mode==='auto') op=before.exists ? 'replace' : 'create';
    else return { status:'error', reason:'invalid_mode', message:`Invalid write mode: ${mode}` };
    if(op==='create' && before.exists) return { status:'conflict', reason:'target_exists', actual_sha256:before.sha256 };
    if(op==='replace' && !before.exists) return { status:'conflict', reason:'target_missing' };
    if(op==='replace') {
      if(!options.expectedSha) return { status:'conflict', reason:'expected_sha256_required', actual_sha256:before.sha256 };
      if(options.expectedSha!==before.sha256) return { status:'conflict', reason:'version_mismatch', expected_sha256:options.expectedSha, actual_sha256:before.sha256 };
    }
    await fs.promises.mkdir(path.dirname(target),{recursive:true});
    const temp=await createDurableTemp(target,content);
    const newSha=crypto.createHash('sha256').update(content,'utf8').digest('hex');
    const id=crypto.randomUUID();
    const record={ id, op, target, temp, expected_sha256:op==='replace'?options.expectedSha:null, new_sha256:newSha, created_at:new Date().toISOString() };
    durableWriteJson(txnJournalPath(id),record);
    try {
      const helper=await runFileTxnHelper(op,target,op==='replace'?options.expectedSha:null,temp);
      if(helper.status==='conflict') {
        await reconcileFileTxn(record,{cleanup:true});
        return helper;
      }
      await fsyncDirectory(path.dirname(target));
      try { await fs.promises.unlink(txnJournalPath(id)); } catch (_) {}
      return { ...helper, transaction_id:id };
    } catch(err) {
      const rec=await reconcileFileTxn(record,{cleanup:true});
      if(rec.state==='completed') return { status:op==='create'?'created':'written', recovered_after_helper_error:true, new_sha256:newSha, bytes:Buffer.byteLength(content,'utf8'), transaction_id:id };
      return { status:'error', reason:'helper_failure', message:err.message, recovery_state:rec.state };
    }
  });
}

async function performTransactionalDelete(target, expectedSha) {
  return withFileMutationLock(target, async () => {
    const before=await targetRegularFileInfo(target);
    if(!before.exists) return { status:'deleted', already_absent:true };
    if(before.symlink) { await fs.promises.unlink(target); await fsyncDirectory(path.dirname(target)); return { status:'deleted', symlink:true }; }
    if(!before.regular) return { status:'error', reason:'not_regular_file', message:`Version-checked delete only supports regular files: ${target}` };
    if(!expectedSha) return { status:'conflict', reason:'expected_sha256_required', actual_sha256:before.sha256 };
    if(expectedSha!==before.sha256) return { status:'conflict', reason:'version_mismatch', expected_sha256:expectedSha, actual_sha256:before.sha256 };
    const id=crypto.randomUUID(); const record={id,op:'delete',target,temp:null,expected_sha256:expectedSha,new_sha256:null,created_at:new Date().toISOString()};
    durableWriteJson(txnJournalPath(id),record);
    try {
      const helper=await runFileTxnHelper('delete',target,expectedSha,null);
      await fsyncDirectory(path.dirname(target)); try { await fs.promises.unlink(txnJournalPath(id)); } catch (_) {}
      return { ...helper, transaction_id:id };
    } catch(err) {
      const rec=await reconcileFileTxn(record,{cleanup:true});
      if(rec.state==='completed') return { status:'deleted', recovered_after_helper_error:true, old_sha256:expectedSha, transaction_id:id };
      return {status:'error',reason:'helper_failure',message:err.message,recovery_state:rec.state};
    }
  });
}


const WORKSPACE_ROOT = process.env.MCPX_WORKSPACE_ROOT || path.join(os.homedir(), '.local', 'share', 'mcpx-workspaces');
const WORKSPACE_LOCK_ROOT = path.join(WORKSPACE_ROOT, '.locks');
fs.mkdirSync(WORKSPACE_ROOT, { recursive:true, mode:0o700 });
fs.mkdirSync(WORKSPACE_LOCK_ROOT, { recursive:true, mode:0o700 });
try { fs.chmodSync(WORKSPACE_ROOT,0o700); fs.chmodSync(WORKSPACE_LOCK_ROOT,0o700); } catch (_) {}
let workspaceControl = null;

function workspacePaths(id) {
  const dir=path.join(WORKSPACE_ROOT,id);
  return {dir,work:path.join(dir,'work'),base:path.join(dir,'base'),manifest:path.join(dir,'manifest.json'),conflicts:path.join(dir,'conflicts'),integrations:path.join(dir,'integrations')};
}
function validateWorkspaceId(id) { return /^[0-9a-f-]{36}$/i.test(String(id||'')); }
function normalizeRelativePath(value) {
  if(typeof value!=='string' || !value.trim()) throw new Error('mutable_paths entries must be non-empty strings');
  if(path.isAbsolute(value)) throw new Error(`mutable path must be relative to project: ${value}`);
  const n=path.normalize(value).replace(/^\.\//,'');
  if(n==='.' || n==='') throw new Error('project root cannot be a mutable path; choose explicit mutable files/directories so bulk data remains read-only');
  if(n==='..' || n.startsWith(`..${path.sep}`)) throw new Error(`mutable path escapes project: ${value}`);
  return n;
}
function compactMutablePaths(values) {
  const raw=[...new Set((values||[]).map(normalizeRelativePath))].sort((a,b)=>a.length-b.length || a.localeCompare(b));
  const out=[];
  for(const v of raw) if(!out.some((p)=>v===p || v.startsWith(`${p}${path.sep}`))) out.push(v);
  if(!out.length) throw new Error('mutable_paths must contain at least one explicit file or directory');
  return out;
}
async function copyPathPreservingLinks(src,dst) {
  const st=await fs.promises.lstat(src);
  await fs.promises.mkdir(path.dirname(dst),{recursive:true});
  if(st.isSymbolicLink()) { const target=await fs.promises.readlink(src); await fs.promises.symlink(target,dst); return; }
  if(st.isDirectory()) { await fs.promises.cp(src,dst,{recursive:true,preserveTimestamps:true,verbatimSymlinks:true,errorOnExist:true}); return; }
  if(st.isFile()) { await fs.promises.copyFile(src,dst,fs.constants.COPYFILE_EXCL); try{await fs.promises.chmod(dst,st.mode & 0o777);}catch(_){} return; }
  throw new Error(`Unsupported mutable path type: ${src}`);
}
function mutableRelation(rel, mutables) {
  let exact=false,desc=false;
  for(const m of mutables) { if(m===rel) exact=true; else if(m.startsWith(`${rel}${path.sep}`)) desc=true; }
  return {exact,desc};
}
async function materializeWorkspaceMirror(project,work,base,mutables,rel='') {
  const canonicalDir=rel?path.join(project,rel):project;
  const workDir=rel?path.join(work,rel):work;
  await fs.promises.mkdir(workDir,{recursive:true,mode:0o700});
  let entries=[]; try{entries=await fs.promises.readdir(canonicalDir,{withFileTypes:true});}catch(err){if(err.code==='ENOENT')return;throw err;}
  for(const e of entries) {
    if(!rel && e.name==='.mcpx-canonical') continue;
    const childRel=rel?path.join(rel,e.name):e.name;
    const canonical=path.join(project,childRel); const dest=path.join(work,childRel); const bdest=path.join(base,childRel);
    const relation=mutableRelation(childRel,mutables);
    if(relation.exact) { await copyPathPreservingLinks(canonical,dest); await copyPathPreservingLinks(canonical,bdest); continue; }
    if(relation.desc) {
      const st=await fs.promises.lstat(canonical);
      if(!st.isDirectory() || st.isSymbolicLink()) throw new Error(`Cannot place mutable descendant beneath non-directory/symlink: ${childRel}`);
      await fs.promises.mkdir(dest,{recursive:true,mode:0o700}); await fs.promises.mkdir(bdest,{recursive:true,mode:0o700});
      await materializeWorkspaceMirror(project,work,base,mutables,childRel); continue;
    }
    await fs.promises.mkdir(path.dirname(dest),{recursive:true});
    await fs.promises.symlink(canonical,dest);
  }
  // Ensure parent directories exist for explicitly mutable paths that are absent in canonical.
  for(const m of mutables) if(rel==='' || m.startsWith(`${rel}${path.sep}`)) {
    const parent=path.dirname(path.join(work,m)); if(parent.startsWith(work)) await fs.promises.mkdir(parent,{recursive:true,mode:0o700});
    const bparent=path.dirname(path.join(base,m)); if(bparent.startsWith(base)) await fs.promises.mkdir(bparent,{recursive:true,mode:0o700});
  }
}

async function pathState(root, rel) {
  const p=path.join(root,rel);
  try {
    const st=await fs.promises.lstat(p);
    if(st.isSymbolicLink()) return {type:'symlink',target:await fs.promises.readlink(p)};
    if(st.isDirectory()) return {type:'dir'};
    if(st.isFile()) return {type:'file',sha256:await sha256File(p),bytes:st.size};
    return {type:'other'};
  } catch(err) { if(err.code==='ENOENT') return {type:'absent'}; throw err; }
}
function stateEqual(a,b) {
  if(!a||!b||a.type!==b.type)return false;
  if(a.type==='file')return a.sha256===b.sha256;
  if(a.type==='symlink')return a.target===b.target;
  return true;
}
async function collectPathStates(root, rel, out) {
  const st=await pathState(root,rel); out[rel]=st;
  if(st.type!=='dir')return;
  const dir=path.join(root,rel); let entries=[]; try{entries=await fs.promises.readdir(dir,{withFileTypes:true});}catch(_){return;}
  for(const e of entries) await collectPathStates(root,path.join(rel,e.name),out);
}
async function collectMutableStates(root,mutables) { const out={}; for(const m of mutables) await collectPathStates(root,m,out); return out; }
async function workspaceChanged(manifest) {
  const wp=workspacePaths(manifest.id); const base=await collectMutableStates(wp.base,manifest.mutable_paths); const work=await collectMutableStates(wp.work,manifest.mutable_paths);
  const keys=new Set([...Object.keys(base),...Object.keys(work)]); const changed=[];
  for(const k of keys) if(!stateEqual(base[k]||{type:'absent'},work[k]||{type:'absent'})) changed.push(k);
  return changed.sort();
}
async function acquireProjectIntegrationLock(project,workspaceId) {
  const key=crypto.createHash('sha256').update(project).digest('hex'); const dir=path.join(WORKSPACE_LOCK_ROOT,key); const owner=path.join(dir,'owner.json');
  const tryOnce=async()=>{try{await fs.promises.mkdir(dir,{mode:0o700});durableWriteJson(owner,{pid:process.pid,workspace_id:workspaceId,project,created_at:new Date().toISOString()});return true;}catch(err){if(err.code!=='EEXIST')throw err;return false;}};
  if(!await tryOnce()) {
    const old=readJson(owner); let alive=false; if(old?.pid){try{process.kill(Number(old.pid),0);alive=true;}catch(_){}}
    if(!alive){await fs.promises.rm(dir,{recursive:true,force:true});if(!await tryOnce())throw new Error('project integration lock could not be acquired after stale-lock cleanup');}
    else throw new Error(`Project integration already active (workspace=${old?.workspace_id || 'unknown'}, pid=${old?.pid})`);
  }
  return async()=>{await fs.promises.rm(dir,{recursive:true,force:true});};
}
async function createDurableTempFromFile(target,source) {
  const dir=path.dirname(target),base=path.basename(target); const tmp=path.join(dir,`.${base}.mcpx-tmp-${process.pid}-${crypto.randomUUID()}`);
  await fs.promises.copyFile(source,tmp,fs.constants.COPYFILE_EXCL); const fh=await fs.promises.open(tmp,'r+');try{await fh.sync();}finally{await fh.close();}return tmp;
}
async function performTransactionalFileFromPath(target,source,mode,expectedSha) {
  return withFileMutationLock(target,async()=>{
    const before=await targetRegularFileInfo(target);
    if(before.symlink || (before.exists&&!before.regular))return {status:'error',reason:'unsafe_target'};
    const op=mode==='create'?'create':'replace';
    if(op==='create'&&before.exists)return {status:'conflict',reason:'target_exists',actual_sha256:before.sha256};
    if(op==='replace'){if(!before.exists)return {status:'conflict',reason:'target_missing'};if(before.sha256!==expectedSha)return {status:'conflict',reason:'version_mismatch',expected_sha256:expectedSha,actual_sha256:before.sha256};}
    await fs.promises.mkdir(path.dirname(target),{recursive:true}); const temp=await createDurableTempFromFile(target,source); const newSha=await sha256File(source); const bytes=(await fs.promises.stat(source)).size; const id=crypto.randomUUID();
    const rec={id,op,target,temp,expected_sha256:op==='replace'?expectedSha:null,new_sha256:newSha,created_at:new Date().toISOString()};durableWriteJson(txnJournalPath(id),rec);
    try{const helper=await runFileTxnHelper(op,target,op==='replace'?expectedSha:null,temp);if(helper.status==='conflict'){await reconcileFileTxn(rec,{cleanup:true});return helper;}await fsyncDirectory(path.dirname(target));try{await fs.promises.unlink(txnJournalPath(id));}catch(_){}return {...helper,bytes,transaction_id:id};}
    catch(err){const rr=await reconcileFileTxn(rec,{cleanup:true});if(rr.state==='completed')return {status:op==='create'?'created':'written',recovered_after_helper_error:true,new_sha256:newSha,bytes,transaction_id:id};return {status:'error',reason:'helper_failure',message:err.message,recovery_state:rr.state};}
  });
}
async function threeWayMergeText(baseFile,currentFile,proposedFile,workspace,rel) {
  const r=await runProcessCapture('/usr/bin/git',['merge-file','--diff3','--stdout','-L','CURRENT','-L','BASE','-L','PROPOSED',currentFile,baseFile,proposedFile],{maxBytes:64*1024*1024,timeoutMs:30000});
  if(r.code===0)return {status:'merged',content:r.stdout};
  if(r.code===1){const cp=path.join(workspacePaths(workspace.id).conflicts,`${rel}.merge`);await fs.promises.mkdir(path.dirname(cp),{recursive:true,mode:0o700});await fs.promises.writeFile(cp,r.stdout,{mode:0o600});return {status:'conflict',reason:'text_merge_conflict',conflict_file:cp};}
  return {status:'error',reason:'merge_failed',message:r.stderr.slice(0,4000)};
}
async function planWorkspaceIntegration(manifest) {
  const wp=workspacePaths(manifest.id),project=manifest.project_path;
  const base=await collectMutableStates(wp.base,manifest.mutable_paths), proposed=await collectMutableStates(wp.work,manifest.mutable_paths), current=await collectMutableStates(project,manifest.mutable_paths);
  const keys=[...new Set([...Object.keys(base),...Object.keys(proposed),...Object.keys(current)])].sort(); const plan=[],conflicts=[];
  for(const rel of keys){const b=base[rel]||{type:'absent'},p=proposed[rel]||{type:'absent'},c=current[rel]||{type:'absent'};
    if(stateEqual(p,b)){plan.push({rel,action:'noop_agent_unchanged',base:b,current:c,proposed:p});continue;}
    if(stateEqual(p,c)){plan.push({rel,action:'noop_already_matches',base:b,current:c,proposed:p});continue;}
    if(stateEqual(c,b)){
      if(p.type==='file')plan.push({rel,action:c.type==='absent'?'create_file':'replace_file',base:b,current:c,proposed:p});
      else if(p.type==='absent'&&c.type==='file')plan.push({rel,action:'delete_file',base:b,current:c,proposed:p});
      else if(p.type==='dir'&&c.type==='absent')plan.push({rel,action:'create_dir',base:b,current:c,proposed:p});
      else if(p.type==='absent'&&c.type==='dir')plan.push({rel,action:'remove_dir_if_empty',base:b,current:c,proposed:p});
      else {const x={rel,reason:'unsupported_type_change',base:b,current:c,proposed:p};conflicts.push(x);plan.push({rel,action:'conflict',...x});}
      continue;
    }
    // Both canonical and agent changed relative to base.
    if(b.type==='file'&&c.type==='file'&&p.type==='file'){
      const bf=path.join(wp.base,rel),cf=path.join(project,rel),pf=path.join(wp.work,rel); const bs=await fs.promises.stat(bf),cs=await fs.promises.stat(cf),ps=await fs.promises.stat(pf);
      const [bt,ct,pt]=await Promise.all([classifyTextFile(bf,bs),classifyTextFile(cf,cs),classifyTextFile(pf,ps)]);
      if(bt.text&&ct.text&&pt.text){const m=await threeWayMergeText(bf,cf,pf,manifest,rel);if(m.status==='merged')plan.push({rel,action:'merge_text',base:b,current:c,proposed:p,merged_content:m.content});else{const x={rel,reason:m.reason,conflict_file:m.conflict_file||null};conflicts.push(x);plan.push({rel,action:'conflict',...x});}}
      else{const x={rel,reason:'binary_both_changed',base:b,current:c,proposed:p};conflicts.push(x);plan.push({rel,action:'conflict',...x});}
    } else {const x={rel,reason:'both_changed_incompatible_types',base:b,current:c,proposed:p};conflicts.push(x);plan.push({rel,action:'conflict',...x});}
  }
  return {plan,conflicts,base,current,proposed};
}
async function applyWorkspacePlan(manifest,planned) {
  const wp=workspacePaths(manifest.id),project=manifest.project_path,applied=[];
  for(const item of planned.plan){const rel=item.rel,target=path.join(project,rel),source=path.join(wp.work,rel);let r=null;
    if(item.action.startsWith('noop_')||item.action==='conflict')continue;
    if(item.action==='create_file')r=await performTransactionalFileFromPath(target,source,'create',null);
    else if(item.action==='replace_file')r=await performTransactionalFileFromPath(target,source,'replace',item.current.sha256);
    else if(item.action==='merge_text')r=await performTransactionalWrite(target,item.merged_content,{mode:'replace',expectedSha:item.current.sha256});
    else if(item.action==='delete_file')r=await performTransactionalDelete(target,item.current.sha256);
    else if(item.action==='create_dir'){await fs.promises.mkdir(target,{recursive:true,mode:0o700});r={status:'created_dir'};}
    else if(item.action==='remove_dir_if_empty'){try{await fs.promises.rmdir(target);r={status:'removed_dir'};}catch(err){r={status:'error',reason:'directory_not_empty_or_changed',message:err.message};}}
    if(r && (r.status==='conflict'||r.status==='error'))return {status:'partial_failure',failed:{rel,action:item.action,result:r},applied};
    if(r)applied.push({rel,action:item.action,result:r});
  }
  return {status:'integrated',applied};
}

const OPERATIONS_DIR = process.env.MCPX_OPERATIONS_DIR || path.join(__dirname, 'operations');
const OPERATION_LOCK_ROOT = path.join(OPERATIONS_DIR, '.locks');
fs.mkdirSync(OPERATIONS_DIR,{recursive:true,mode:0o700});fs.mkdirSync(OPERATION_LOCK_ROOT,{recursive:true,mode:0o700});
try{fs.chmodSync(OPERATIONS_DIR,0o700);fs.chmodSync(OPERATION_LOCK_ROOT,0o700);}catch(_){}

// Compact, long-lived recovery plane. Normal job/operation retention remains unchanged;
// these records preserve enough lifecycle state to reconcile hidden ChatGPT tool returns.
const RECOVERY_DIR = process.env.MCPX_RECOVERY_DIR || path.join(__dirname, 'recovery');
const RECOVERY_JOB_DIR = path.join(RECOVERY_DIR, 'jobs');
const RECOVERY_OPERATION_DIR = path.join(RECOVERY_DIR, 'operations');
const RECOVERY_EVENT_LOG = path.join(RECOVERY_DIR, 'events.jsonl');
for (const d of [RECOVERY_DIR, RECOVERY_JOB_DIR, RECOVERY_OPERATION_DIR]) {
  fs.mkdirSync(d, { recursive: true, mode: 0o700 });
  try { fs.chmodSync(d, 0o700); } catch (_) {}
}
function recoveryOperationPath(id){return path.join(RECOVERY_OPERATION_DIR,`${operationKey(id)}.json`);}
function recoveryJobPath(id){return path.join(RECOVERY_JOB_DIR,`${id}.json`);}
function recoveryTail(file,maxBytes=2048){
  try{const size=fs.statSync(file).size;if(!size)return'';const n=Math.min(size,maxBytes),fd=fs.openSync(file,'r');try{const b=Buffer.alloc(n);const got=fs.readSync(fd,b,0,n,size-n);return b.subarray(0,got).toString('utf8');}finally{fs.closeSync(fd);}}catch(_){return'';}
}
function recoveryFileSha(file){try{return crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');}catch(_){return null;}}
function safeRecoveryWriteJson(file,value){try{atomicWriteJson(file,value);return true;}catch(err){console.error('[recovery write]',err.message);return false;}}
function appendRecoveryEvent(event,payload={}){try{fs.appendFileSync(RECOVERY_EVENT_LOG,`${JSON.stringify({ts:new Date().toISOString(),event,...payload})}\n`,{encoding:'utf8',mode:0o600});}catch(err){console.error('[recovery event]',err.message);}}
function compactRecoveryOperationSummary(tool,result,rec){
  const sc=(result?.structuredContent&&typeof result.structuredContent==='object')?result.structuredContent:{};
  const jobId=sc.job_id || (JOB_CREATING_TOOLS.has(tool)?rec?.logical_id:null) || null;
  return {
    job_id:jobId, workspace_id:sc.workspace_id||null, path:sc.path||null, cwd:sc.cwd||null,
    status:sc.status||null, exit_code:sc.exitCode ?? sc.exit_code ?? null, signal:sc.signal||null,
    transaction_id:sc.transaction_id||null, old_sha256:sc.old_sha256||null, new_sha256:sc.new_sha256||null,
    bytes:sc.bytes ?? null, recovered_operation:!!sc.recovered_operation,
    stdout_tail:typeof sc.stdout==='string'?sc.stdout.slice(-2048):null,
    stderr_tail:typeof sc.stderr==='string'?sc.stderr.slice(-2048):null,
  };
}
function persistRecoveryOperation(rec,result=null){
  try{
    if(!rec?.operation_id)return;
    const file=recoveryOperationPath(rec.operation_id),old=readJson(file);
    const summary=result?compactRecoveryOperationSummary(rec.tool,result,rec):(old?.summary||compactRecoveryOperationSummary(rec.tool,null,rec));
    const resultSha=rec.result_sha256 || (result?crypto.createHash('sha256').update(JSON.stringify(result)).digest('hex'):old?.result_sha256||null);
    const out={operation_id:rec.operation_id,tool:rec.tool,fingerprint:rec.fingerprint,state:rec.state,logical_id:rec.logical_id||null,created_at:rec.created_at||old?.created_at||null,claimed_at:rec.claimed_at||old?.claimed_at||null,completed_at:rec.completed_at||old?.completed_at||null,uncertain_at:rec.uncertain_at||old?.uncertain_at||null,uncertain_reason:rec.uncertain_reason||old?.uncertain_reason||null,result_sha256:resultSha,summary,updated_at:new Date().toISOString()};
    if(safeRecoveryWriteJson(file,out)&&old?.state!==out.state)appendRecoveryEvent(`OPERATION_${String(out.state||'unknown').toUpperCase()}`,{operation_id:out.operation_id,tool:out.tool,logical_id:out.logical_id,state:out.state,result_sha256:out.result_sha256});
  }catch(err){console.error('[recovery operation]',err.message);}
}
function persistRecoveryJob(job){
  try{
    if(!job?.id)return;
    const file=recoveryJobPath(job.id),old=readJson(file),terminal=job.status!=='running';
    const out={job_id:job.id,operation_id:job.operationId||old?.operation_id||null,kind:job.kind||'background',cwd:job.cwd||null,command_bytes:Buffer.byteLength(job.command||'','utf8'),command_sha256:crypto.createHash('sha256').update(job.command||'').digest('hex'),pid:job.pid||null,status:job.status,exit_code:job.exitCode??null,signal:job.signal||null,primary_exit_code:job.primaryExitCode??null,primary_signal:job.primaryExitSignal??null,started_at:job.startedAt||null,ended_at:job.endedAt||null,timed_out:!!job.timedOut,stdout_bytes:outputLength(job.paths.stdout),stderr_bytes:outputLength(job.paths.stderr),updated_at:new Date().toISOString()};
    if(terminal){out.stdout_sha256=recoveryFileSha(job.paths.stdout);out.stderr_sha256=recoveryFileSha(job.paths.stderr);out.stdout_tail=recoveryTail(job.paths.stdout);out.stderr_tail=recoveryTail(job.paths.stderr);}
    if(!safeRecoveryWriteJson(file,out))return;
    if(!old)appendRecoveryEvent('JOB_STARTED',{job_id:out.job_id,operation_id:out.operation_id,kind:out.kind,cwd:out.cwd,started_at:out.started_at,command_sha256:out.command_sha256});
    if(terminal&&old?.status==='running')appendRecoveryEvent(`JOB_${String(out.status||'terminal').toUpperCase()}`,{job_id:out.job_id,operation_id:out.operation_id,status:out.status,exit_code:out.exit_code,signal:out.signal,ended_at:out.ended_at,stdout_sha256:out.stdout_sha256,stderr_sha256:out.stderr_sha256});
  }catch(err){console.error('[recovery job]',err.message);}
}
function backfillRecoveryOperations(){
  try{for(const n of fs.readdirSync(OPERATIONS_DIR)){if(n==='.locks')continue;const d=path.join(OPERATIONS_DIR,n),m=readJson(path.join(d,'meta.json'));if(m?.operation_id)persistRecoveryOperation(m,readJson(path.join(d,'result.json')));}}catch(err){console.error('[recovery backfill operations]',err.message);}
}
const SIDE_EFFECTING_TOOLS = new Set(['write_file','edit_file','delete_file','start_command','execute_command','send_command_input','stop_command','create_agent_workspace','start_workspace_command','start_verified_workspace_producer','start_verified_workspace_consumer','integrate_agent_workspace','remove_agent_workspace','resume_workspace']);
const JOB_CREATING_TOOLS = new Set(['start_command','execute_command','start_workspace_command','start_verified_workspace_producer','start_verified_workspace_consumer']);
const RESOURCE_CREATING_TOOLS = new Set(['create_agent_workspace']);
const COMMAND_START_TOOLS = new Set(['start_command','execute_command','start_workspace_command','start_verified_workspace_producer','start_verified_workspace_consumer','send_command_input']);
function canonicalOperationValue(v){if(Array.isArray(v))return v.map(canonicalOperationValue);if(v&&typeof v==='object'){const o={};for(const k of Object.keys(v).sort()){if(k==='operation_id'||k.startsWith('__mcpx_'))continue;o[k]=canonicalOperationValue(v[k]);}return o;}return v;}
function operationFingerprint(tool,args){return crypto.createHash('sha256').update(JSON.stringify({tool,args:canonicalOperationValue(args||{})})).digest('hex');}
function operationKey(id){return crypto.createHash('sha256').update(String(id)).digest('hex');}
function operationPaths(id){const dir=path.join(OPERATIONS_DIR,operationKey(id));return{dir,meta:path.join(dir,'meta.json'),result:path.join(dir,'result.json')}}
function operationResultError(message,extra={}){return{isError:true,content:[{type:'text',text:message}],structuredContent:{...extra,text:message}};}
async function acquireOperationLock(operationId,waitMs=30000){
  const key=operationKey(operationId),dir=path.join(OPERATION_LOCK_ROOT,key),owner=path.join(dir,'owner.json'),deadline=Date.now()+waitMs;
  while(true){
    try{await fs.promises.mkdir(dir,{mode:0o700});durableWriteJson(owner,{pid:process.pid,server_instance_id:MCPX_SERVER_INSTANCE_ID,operation_id:operationId,created_at:new Date().toISOString()});return async()=>{await fs.promises.rm(dir,{recursive:true,force:true});};}
    catch(err){if(err.code!=='EEXIST')throw err;const old=readJson(owner);let alive=false;if(old?.pid){try{process.kill(Number(old.pid),0);alive=true;}catch(_){}}
      if(!alive){await fs.promises.rm(dir,{recursive:true,force:true});continue;}if(Date.now()>=deadline)throw new Error(`operation ${operationId} is busy in pid ${old?.pid}`);await new Promise(r=>setTimeout(r,50));}
  }
}
function validOperationId(id){return typeof id==='string'&&id.length>=8&&id.length<=160&&/^[A-Za-z0-9._:-]+$/.test(id);}
function recoveredWorkspaceIdFromArgs(args, key, job=null){
  try{const h=workspaceControl.decodeWorkspaceHandle(args?.[key]);if(h?.workspace_id)return h.workspace_id;}catch(_){}
  if(job?.cwd){const rel=path.relative(WORKSPACE_ROOT,path.resolve(job.cwd));const first=rel.split(path.sep)[0];if(validateWorkspaceId(first))return first;}
  return null;
}
function recoveredFreshWorkspaceFields(wid){
  if(!wid)return{};const rec=workspaceControl.tombstoneInfo(wid),wp=workspacePaths(wid);return{workspace_id:wid,workspace_path:wp.work,workspace_handle:rec?.workspace_handle||null,canonical_project:rec?.project_path||null};
}
function recoveredJobResult(tool,job,args){
  refreshJob(job); if(!job.registered)registerJob(job); const summary=commandJobSummary(job);
  if(tool==='start_command')return{content:[{type:'text',text:JSON.stringify(summary,null,2)}],structuredContent:{...summary,recovered_operation:true}};
  if(tool==='execute_command'){
    const wid=recoveredWorkspaceIdFromArgs(args,'workspace_handle',job),ws=recoveredFreshWorkspaceFields(wid);const payload={promoted_to_background:true,recovered_after_interrupted_response:true,message:'The original execute_command may have lost its response; MCPX recovered the same durable job instead of replaying the command.',...summary,...ws,recovered_operation:true};return{content:[{type:'text',text:JSON.stringify(payload,null,2)}],structuredContent:payload};
  }
  if(tool==='start_workspace_command'){
    const wid=recoveredWorkspaceIdFromArgs(args,'workspace_handle',job),ws=recoveredFreshWorkspaceFields(wid);const payload={...summary,...ws,network:args.network===true,recovered_operation:true};return{content:[{type:'text',text:JSON.stringify(payload,null,2)}],structuredContent:payload};
  }
  if(tool==='start_verified_workspace_producer'){
    const wid=recoveredWorkspaceIdFromArgs(args,'workspace_handle',job),ws=recoveredFreshWorkspaceFields(wid);const payload={...summary,...ws,pipeline:args.pipeline,gate_path:args.gate_path,verified_role:'producer',network:args.network===true,recovered_operation:true};return{content:[{type:'text',text:JSON.stringify(payload,null,2)}],structuredContent:payload};
  }
  if(tool==='start_verified_workspace_consumer'){
    const wid=recoveredWorkspaceIdFromArgs(args,'workspace_handle',job),cwid=recoveredWorkspaceIdFromArgs(args,'candidate_workspace_handle',null),ws=recoveredFreshWorkspaceFields(wid),cws=recoveredFreshWorkspaceFields(cwid);const payload={...summary,...ws,candidate_workspace_id:cwid,candidate_workspace_path:cws.workspace_path||null,candidate_workspace_handle:cws.workspace_handle||null,candidate_build_id:args.candidate_build_id||null,pipeline:args.pipeline,gate_path:args.gate_path,verified_role:'consumer',network:args.network===true,recovered_operation:true};return{content:[{type:'text',text:JSON.stringify(payload,null,2)}],structuredContent:payload};
  }
  return{content:[{type:'text',text:JSON.stringify({recovered_after_interrupted_response:true,...summary},null,2)}],structuredContent:{recovered_after_interrupted_response:true,...summary}};
}
async function recoverClaimedOperation(rec,tool,args,paths){
  const saved=readJson(paths.result); if(saved)return{result:saved};
  if(JOB_CREATING_TOOLS.has(tool)){
    const id=rec.logical_id,job=id?commandJobs.get(id):null;if(job)return{result:recoveredJobResult(tool,job,args)};
    if(id&&fs.existsSync(jobPaths(id).dir))return{uncertain:true,reason:'job_directory_exists_but_job_state_could_not_be_rehydrated'};
    return{retrySafe:true};
  }
  if(RESOURCE_CREATING_TOOLS.has(tool)){
    const id=rec.logical_id,wp=id?workspacePaths(id):null,manifest=wp?readJson(wp.manifest):null;
    if(manifest){workspaceControl.bootstrap();const wrec=workspaceControl.tombstoneInfo(id);return{result:{content:[{type:'text',text:`Recovered already-created isolated workspace ${id} at ${wp.work}`}],structuredContent:{workspace_id:id,workspace_path:wp.work,canonical_project:manifest.project_path,canonical_link:path.join(wp.work,'.mcpx-canonical'),mutable_paths:manifest.mutable_paths,status:manifest.status,workspace_handle:wrec?.workspace_handle||null,project_state_token:workspaceControl.currentProjectToken(manifest.project_path),recovered_operation:true}}};}
    if(wp&&!fs.existsSync(wp.dir))return{retrySafe:true};
    return{uncertain:true,reason:'workspace_directory_exists_without_complete_manifest'};
  }
  return{uncertain:true,reason:'side_effect_may_have_started'};
}
async function beginSideEffectOperation(tool,args){
  if(!SIDE_EFFECTING_TOOLS.has(tool))return null;
  const operationId=args?.operation_id;
  if(!validOperationId(operationId))return{early:operationResultError(`operation_id is required for ${tool} and must be an 8-160 character token. Reuse the same operation_id only when retrying the same logical call.`,{tool,state:'operation_id_required'})};
  const release=await acquireOperationLock(operationId);const paths=operationPaths(operationId);await fs.promises.mkdir(paths.dir,{recursive:true,mode:0o700});const fingerprint=operationFingerprint(tool,args);let rec=readJson(paths.meta);
  if(rec&& (rec.tool!==tool||rec.fingerprint!==fingerprint)){await release();return{early:operationResultError(`Idempotency conflict: operation_id ${operationId} was already used with different tool arguments.`,{operation_id:operationId,state:'idempotency_conflict',tool,original_tool:rec.tool})};}
  if(readJson(paths.result)){const result=readJson(paths.result);if(rec?.state!=='completed'){rec={...rec,state:'completed',completed_at:new Date().toISOString(),recovered_result_file:true};durableWriteJson(paths.meta,rec);}persistRecoveryOperation(rec,result);await release();return{early:result};}
  if(rec?.state==='completed'){await release();return{early:operationResultError(`Operation ${operationId} is marked completed but its durable result is missing; refusing replay.`,{operation_id:operationId,state:'uncertain',tool})};}
  if(rec?.state==='uncertain'){persistRecoveryOperation(rec,null);await release();return{early:operationResultError(`Operation ${operationId} is in uncertain state after an interrupted side effect; MCPX will not replay it automatically. Inspect the target/job/workspace and use a NEW operation_id only after deciding the desired next action.`,{operation_id:operationId,state:'uncertain',tool,reason:rec.uncertain_reason||null,logical_id:rec.logical_id||null})};}
  if(rec?.state==='claimed'){
    const recovery=await recoverClaimedOperation(rec,tool,args,paths);
    if(recovery.result){const decorated=decorateOperationResult(recovery.result,operationId,true,tool);durableWriteJson(paths.result,decorated);rec={...rec,state:'completed',completed_at:new Date().toISOString(),recovered_after_interruption:true};durableWriteJson(paths.meta,rec);persistRecoveryOperation(rec,decorated);await release();return{early:decorated};}
    if(!recovery.retrySafe){rec={...rec,state:'uncertain',uncertain_at:new Date().toISOString(),uncertain_reason:recovery.reason||'unknown'};durableWriteJson(paths.meta,rec);persistRecoveryOperation(rec,null);await release();return{early:operationResultError(`Operation ${operationId} became uncertain after interruption; automatic replay is disabled.`,{operation_id:operationId,state:'uncertain',tool,reason:rec.uncertain_reason,logical_id:rec.logical_id||null})};}
    rec={...rec,state:'reserved',recovered_safe_to_retry:true,recovered_at:new Date().toISOString()};durableWriteJson(paths.meta,rec);
  }
  if(!rec){rec={operation_id:operationId,tool,fingerprint,state:'reserved',created_at:new Date().toISOString(),logical_id:(JOB_CREATING_TOOLS.has(tool)||RESOURCE_CREATING_TOOLS.has(tool))?crypto.randomUUID():null};durableWriteJson(paths.meta,rec);}
  rec={...rec,state:'claimed',claimed_at:new Date().toISOString(),claim_owner_pid:process.pid,claim_owner_instance:MCPX_SERVER_INSTANCE_ID};durableWriteJson(paths.meta,rec);persistRecoveryOperation(rec,null);
  if(process.env.MCPX_TEST_OPERATION_CLAIM_MARKER){fs.writeFileSync(process.env.MCPX_TEST_OPERATION_CLAIM_MARKER,JSON.stringify(rec),{mode:0o600});const delay=Number(process.env.MCPX_TEST_OPERATION_CLAIM_DELAY_MS)||0;if(delay)await new Promise(r=>setTimeout(r,delay));}
  if(rec.logical_id)args.__mcpx_logical_id=rec.logical_id;
  return{operation_id:operationId,tool,fingerprint,record:rec,paths,release};
}
const STRUCTURED_COMPAT_HEADROOM_BYTES = 256 * 1024;
const LOSSLESS_TEXT_RESULT_TOOLS = new Set(['read_file','read_pdf_text','read_result_chunk']);
function compactStructuredSummary(tool, structured, originalTextBytes){
  if(tool==='read_file') return `Returned ${Buffer.byteLength(String(structured.text ?? ''),'utf8')} text bytes from ${structured.path || 'file'}; full payload is in structuredContent.text.`;
  if(tool==='read_pdf_text') return `Extracted ${structured.extracted_text_bytes ?? originalTextBytes} text bytes from ${structured.path || 'PDF'}; full payload is in structuredContent.text.`;
  if(tool==='read_result_chunk') return `Returned ${structured.returned_bytes ?? originalTextBytes} bytes from result ${structured.result_id || ''}; full payload is in structuredContent.text.`;
  if(tool==='list_directory') return `Listed ${structured.returned ?? structured.items?.length ?? 0} of ${structured.total_entries ?? structured.items?.length ?? 0} directory entries; full listing is in structuredContent.items.`;
  if(tool==='list_command_jobs') return `Listed ${structured.returned ?? structured.items?.length ?? 0} of ${structured.total_jobs ?? structured.items?.length ?? 0} recent jobs; full listing is in structuredContent.items.`;
  if(tool==='execute_command') return `Command completed with exitCode=${structured.exitCode ?? 'unknown'}; stdout/stderr and command metadata are in structuredContent.`;
  if(tool==='start_verified_workspace_producer'||tool==='start_verified_workspace_consumer') return `Verified ${structured.verified_role || 'pipeline'} job ${structured.job_id || ''} started for pipeline=${structured.pipeline || ''}; full dependency/gate metadata is in structuredContent.`;
  if(tool==='command_status') return `Job ${structured.job_id || ''} status=${structured.status || 'unknown'} exit_code=${structured.exit_code ?? 'n/a'}; incremental output and metadata are in structuredContent.`;
  if(tool==='get_file_info') return `File metadata returned for ${structured.path || 'path'}; full metadata is in structuredContent.`;
  if(tool==='agent_workspace_info') return `Workspace ${structured.id || structured.workspace_id || ''} status=${structured.status || 'unknown'}; full state is in structuredContent.`;
  return `MCPX ${tool || 'tool'} returned structured data; full result is in structuredContent.`;
}
function ensureStructuredContentCompatibility(result,tool=null){
  if(!result || typeof result !== 'object') return result;
  const out={...result};
  const structured=(result.structuredContent && typeof result.structuredContent==='object' && !Array.isArray(result.structuredContent)) ? {...result.structuredContent} : {};
  const textBlocks=Array.isArray(result.content) ? result.content.filter(block=>block && block.type==='text' && typeof block.text==='string') : [];
  let contentIsStructuredMirror=false;
  if(textBlocks.length){
    const text=textBlocks.map(block=>block.text).join(textBlocks.length>1?'\n':'');
    if(LOSSLESS_TEXT_RESULT_TOOLS.has(tool)){
      // ChatGPT's current MCP adapter exposes these lossless text payloads to the model through structuredContent.
      // Preserve the compatibility mirror even though it duplicates content; bulk pressure is controlled separately.
      structured.text=textBlocks[0].text;
      contentIsStructuredMirror=true;
      if(textBlocks.length>1) structured.notices=textBlocks.slice(1).map(block=>block.text);
    } else {
      let parsed;
      if(textBlocks.length===1){try{parsed=JSON.parse(text);}catch(_){}}
      if(parsed && typeof parsed==='object' && !Array.isArray(parsed)){
        contentIsStructuredMirror=true;
        for(const [key,value] of Object.entries(parsed)) if(!Object.prototype.hasOwnProperty.call(structured,key)) structured[key]=value;
      } else if(Array.isArray(parsed)) {
        contentIsStructuredMirror=true;
        if(!Object.prototype.hasOwnProperty.call(structured,'items')) structured.items=parsed;
      } else if(Object.keys(structured).length===0 && !Object.prototype.hasOwnProperty.call(structured,'text')) {
        // Content-only errors/messages still get a structured fallback for outputSchema compatibility.
        structured.text=text;
      }
    }
  }
  out.structuredContent=structured;
  if(contentIsStructuredMirror){
    out.content=[{type:'text',text:compactStructuredSummary(tool,structured,Buffer.byteLength(textBlocks[0]?.text||'','utf8'))}];
  }
  return out;
}
function decorateOperationResult(result,operationId,recovered=false,tool=null){const out=ensureStructuredContentCompatibility(result,tool);out.structuredContent={...(out?.structuredContent&&typeof out.structuredContent==='object'?out.structuredContent:{}),operation_id:operationId,...(recovered?{recovered_operation:true}:{})};return out;}
async function finishSideEffectOperation(op,result){if(!op)return result;const decorated=decorateOperationResult(result,op.operation_id,false,op.tool);durableWriteJson(op.paths.result,decorated);const meta={...op.record,state:'completed',completed_at:new Date().toISOString(),result_sha256:crypto.createHash('sha256').update(JSON.stringify(decorated)).digest('hex')};durableWriteJson(op.paths.meta,meta);persistRecoveryOperation(meta,decorated);await op.release();return decorated;}
async function abandonOperationLock(op){if(op?.release)await op.release();}
setInterval(async()=>{const cutoff=Date.now()-30*24*60*60*1000;let names=[];try{names=await fs.promises.readdir(OPERATIONS_DIR);}catch(_){return;}for(const n of names){if(n==='.locks')continue;const dir=path.join(OPERATIONS_DIR,n),m=readJson(path.join(dir,'meta.json'));const t=Date.parse(m?.completed_at||m?.uncertain_at||m?.created_at||'');if(Number.isFinite(t)&&t<cutoff)try{await fs.promises.rm(dir,{recursive:true,force:true});}catch(_){}}},60*60*1000).unref();

function resultPaths(id) {
  const dir = path.join(RESULT_DIR, id);
  return { dir, json: path.join(dir, 'response.json'), text: path.join(dir, 'content.txt'), meta: path.join(dir, 'meta.json') };
}

function spillJsonRpcResult(body, receiptContext) {
  const id = crypto.randomUUID();
  const paths = resultPaths(id);
  fs.mkdirSync(paths.dir, { recursive: true, mode: 0o700 });
  fs.writeFileSync(paths.json, body, { mode: 0o600 });
  let extractedText = '';
  try {
    const parsed = JSON.parse(body.toString('utf8'));
    const structured = parsed?.result?.structuredContent;
    const blocks = Array.isArray(parsed?.result?.content) ? parsed.result.content : [];
    if (LOSSLESS_TEXT_RESULT_TOOLS.has(receiptContext?.tool)) {
      extractedText = blocks.filter((x) => x && x.type === 'text' && typeof x.text === 'string').map((x) => x.text).join('\n\n');
    } else if (structured && typeof structured === 'object' && !Array.isArray(structured)) {
      if (typeof structured.text === 'string') extractedText = structured.text;
      else extractedText = JSON.stringify(structured, null, 2);
    }
    if (!extractedText) extractedText = blocks.filter((x) => x && x.type === 'text' && typeof x.text === 'string').map((x) => x.text).join('\n\n');
  } catch (_) {}
  if (extractedText) fs.writeFileSync(paths.text, extractedText, { mode: 0o600 });
  const meta = {
    result_id: id,
    created_at: new Date().toISOString(),
    tool: receiptContext?.tool || null,
    original_serialized_bytes: body.length,
    response_sha256: crypto.createHash('sha256').update(body).digest('hex'),
    extracted_text_bytes: Buffer.byteLength(extractedText, 'utf8'),
    response_file: paths.json,
    text_file: extractedText ? paths.text : null,
  };
  atomicWriteJson(paths.meta, meta);
  return meta;
}

function compactOverflowToolSummary(tool, structured){
  if(!structured || typeof structured!=='object' || Array.isArray(structured)) return null;
  if(tool==='execute_command') return {exitCode:structured.exitCode ?? null,timedOut:!!structured.timedOut,stdout_total_bytes:structured.stdout_total_bytes ?? null,stderr_total_bytes:structured.stderr_total_bytes ?? null,stdout_prefix:String(structured.stdout||'').slice(0,2048),stderr_prefix:String(structured.stderr||'').slice(0,2048),operation_id:structured.operation_id||null};
  if(tool==='command_status') return {job_id:structured.job_id||null,status:structured.status||null,exit_code:structured.exit_code ?? null,stdout_length:structured.stdout_length ?? null,stderr_length:structured.stderr_length ?? null,stdout_next_offset:structured.stdout_next_offset ?? null,stderr_next_offset:structured.stderr_next_offset ?? null,stdout_prefix:String(structured.stdout||'').slice(0,2048),stderr_prefix:String(structured.stderr||'').slice(0,2048)};
  if(tool==='list_command_jobs') return {total_jobs:structured.total_jobs ?? null,returned:structured.returned ?? structured.items?.length ?? null,truncated:!!structured.truncated,next_cursor:structured.next_cursor||null,items:(structured.items||[]).slice(0,5).map(j=>({job_id:j.job_id,status:j.status,exit_code:j.exit_code,started_at:j.started_at,ended_at:j.ended_at,command_bytes:j.command_bytes,command_sha256:j.command_sha256}))};
  if(tool==='list_directory') return {total_entries:structured.total_entries ?? null,returned:structured.returned ?? structured.items?.length ?? null,truncated:!!structured.truncated,next_cursor:structured.next_cursor||null,items:(structured.items||[]).slice(0,10).map(x=>({name:x.name,path:x.path,type:x.type,size:x.size}))};
  return null;
}
function compactPreservedResponse(originalBody, spill, reason, limitBytes, receiptContext=null) {
  let rpcId = null, originalStructured=null;
  try { const parsed=JSON.parse(originalBody.toString('utf8')); rpcId=parsed?.id ?? null; originalStructured=parsed?.result?.structuredContent ?? null; } catch (_) {}
  const isTransport = reason === 'configured_transport_response_limit';
  const isSemantic = reason === 'semantic_model_facing_response_budget';
  const label = isTransport ? 'configured MCP transport response limit' : (isSemantic ? 'MCPX semantic response soft budget' : 'MCPX model-facing response budget');
  const tool=receiptContext?.tool||null;
  const toolSummary=compactOverflowToolSummary(tool,originalStructured);
  const summaryText=toolSummary ? compactStructuredSummary(tool,originalStructured,spill.original_serialized_bytes) : null;
  const message = `${summaryText ? summaryText+' ' : ''}The complete serialized result (${spill.original_serialized_bytes} bytes) exceeds the ${label}${limitBytes ? ` (${limitBytes} bytes)` : ''}. MCPX preserved it exactly as result_id=${spill.result_id} (sha256=${spill.response_sha256}). Process the local result artifact or use bounded result chunks only if more detail is needed.`;
  return Buffer.from(JSON.stringify({jsonrpc:'2.0',id:rpcId,result:{resultType:'complete',content:[{type:'text',text:message}],structuredContent:{overflow:true,overflow_reason:reason,tool,result_id:spill.result_id,original_serialized_bytes:spill.original_serialized_bytes,limit_bytes:limitBytes||null,sha256:spill.response_sha256,extracted_text_bytes:spill.extracted_text_bytes,tool_summary:toolSummary,text:message}}}), 'utf8');
}

async function sha256File(file) {
  return await new Promise((resolve, reject) => {
    const h = crypto.createHash('sha256');
    const stream = fs.createReadStream(file);
    stream.on('data', (chunk) => h.update(chunk));
    stream.on('error', reject);
    stream.on('end', () => resolve(h.digest('hex')));
  });
}

async function classifyTextFile(file, stat) {
  const sampleBytes = Math.min(stat.size, 64 * 1024);
  const fh = await fs.promises.open(file, 'r');
  let sample;
  try {
    sample = Buffer.alloc(sampleBytes);
    const { bytesRead } = await fh.read(sample, 0, sampleBytes, 0);
    sample = sample.subarray(0, bytesRead);
  } finally { await fh.close(); }
  const ascii = sample.subarray(0, 20).toString('latin1');
  const hex = sample.subarray(0, 8).toString('hex');
  if (ascii.startsWith('%PDF-')) return { text: false, kind: 'pdf', reason: 'PDF document' };
  if (ascii.startsWith('PAR1')) return { text: false, kind: 'parquet', reason: 'Parquet container' };
  if (ascii.startsWith('PK\x03\x04')) return { text: false, kind: 'zip', reason: 'ZIP/container format' };
  if (ascii.startsWith('SQLite format 3')) return { text: false, kind: 'sqlite', reason: 'SQLite database' };
  if (hex.startsWith('89504e470d0a1a0a')) return { text: false, kind: 'png', reason: 'PNG image' };
  if (hex.startsWith('ffd8ff')) return { text: false, kind: 'jpeg', reason: 'JPEG image' };
  if (hex.startsWith('1f8b08')) return { text: false, kind: 'gzip', reason: 'gzip-compressed data' };
  try {
    new TextDecoder('utf-8', { fatal: true }).decode(sample);
    return { text: true, kind: 'text', reason: 'valid UTF-8 sample' };
  } catch (_) {
    return { text: false, kind: 'binary', reason: 'not valid UTF-8 text' };
  }
}

function runProcessCapture(command, args, options = {}) {
  const maxBytes = options.maxBytes || 32 * 1024 * 1024;
  const timeoutMs = options.timeoutMs || 60000;
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { cwd: options.cwd || DEFAULT_BASE_DIR, stdio: ['ignore', 'pipe', 'pipe'], env: process.env });
    const out = []; const err = []; let outBytes = 0; let errBytes = 0; let killedForSize = false;
    const timer = setTimeout(() => { try { child.kill('SIGKILL'); } catch (_) {} }, timeoutMs); timer.unref();
    child.stdout.on('data', (b) => {
      outBytes += b.length;
      if (outBytes > maxBytes) { killedForSize = true; try { child.kill('SIGKILL'); } catch (_) {} return; }
      out.push(b);
    });
    child.stderr.on('data', (b) => { errBytes += b.length; if (errBytes <= 4 * 1024 * 1024) err.push(b); });
    child.on('error', (e) => { clearTimeout(timer); reject(e); });
    child.on('exit', (code, signal) => {
      clearTimeout(timer);
      resolve({ code, signal, stdout: Buffer.concat(out).toString('utf8'), stderr: Buffer.concat(err).toString('utf8'), killedForSize, stdoutBytes: outBytes });
    });
  });
}

function rotateReceiptLogIfNeeded() {
  try {
    const st = fs.statSync(RECEIPT_LOG);
    if (st.size < RECEIPT_MAX_BYTES) return;
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    fs.renameSync(RECEIPT_LOG, `${RECEIPT_LOG}.${stamp}`);
  } catch (err) {
    if (err.code !== 'ENOENT') console.error('[receipt rotate]', err.message);
  }
}

function appendReceipt(entry) {
  try {
    rotateReceiptLogIfNeeded();
    const line = `${JSON.stringify({
      server_instance_id: MCPX_SERVER_INSTANCE_ID,
      server_version: MCPX_SERVER_VERSION,
      build_id: MCPX_BUILD_ID,
      ...entry,
    })}\n`;
    fs.appendFileSync(RECEIPT_LOG, line, { mode: 0o600 });
    try { fs.chmodSync(RECEIPT_LOG, 0o600); } catch (_) {}
  } catch (err) {
    console.error('[receipt write]', err.message);
  }
}

function safeArgMetadata(tool, args) {
  if (!args || typeof args !== 'object') return {};
  const out = {};
  const copyString = (key, max = 1024) => {
    if (typeof args[key] === 'string') out[key] = args[key].slice(0, max);
  };
  for (const key of ['path', 'cwd', 'job_id', 'query', 'operation_id', 'result_id', 'format', 'mode', 'expected_sha256', 'cursor', 'workspace_id', 'project_path', 'pipeline', 'gate_path', 'candidate_build_id', 'branch_role']) copyString(key);
  for (const key of ['workspace_handle','candidate_workspace_handle','project_state_token','branch_of_handle']) {
    if (typeof args[key] === 'string') { out[`${key}_sha256`] = crypto.createHash('sha256').update(args[key]).digest('hex'); out[`${key}_bytes`] = Buffer.byteLength(args[key],'utf8'); }
  }
  for (const key of ['offset', 'limit', 'max_depth', 'timeout_ms', 'force', 'recursive', 'close_stdin', 'keep_stdin_open', 'offset_bytes', 'limit_bytes', 'start_page', 'end_page', 'layout']) {
    if (args[key] !== undefined && ['string','number','boolean'].includes(typeof args[key])) out[key] = args[key];
  }
  if (typeof args.command === 'string') {
    out.command_bytes = Buffer.byteLength(args.command, 'utf8');
    out.command_sha256 = crypto.createHash('sha256').update(args.command).digest('hex');
  }
  if (typeof args.content === 'string') {
    out.content_bytes = Buffer.byteLength(args.content, 'utf8');
    out.content_sha256 = crypto.createHash('sha256').update(args.content).digest('hex');
  }
  if (typeof args.input === 'string') {
    out.input_bytes = Buffer.byteLength(args.input, 'utf8');
    out.input_sha256 = crypto.createHash('sha256').update(args.input).digest('hex');
  }
  return out;
}

function buildReceiptContext(req, parsedBody, requestBytes) {
  if (!parsedBody || parsedBody.method !== 'tools/call') return null;
  const params = parsedBody.params || {};
  const tool = params.name || req.headers['mcp-name'] || null;
  return {
    startedAt: Date.now(),
    tool,
    protocolVersion: req.headers['mcp-protocol-version'] || params?._meta?.['io.modelcontextprotocol/protocolVersion'] || null,
    requestBytes,
    rpcId: parsedBody.id ?? null,
    args: safeArgMetadata(tool, params.arguments || {}),
  };
}

function resolvePath(inputPath) {
  if (!inputPath) return DEFAULT_BASE_DIR;
  if (path.isAbsolute(inputPath)) return path.normalize(inputPath);
  return path.normalize(path.join(DEFAULT_BASE_DIR, inputPath));
}

const commandJobs = new Map();
const MAX_JOB_BUFFER_BYTES = 10 * 1024 * 1024;
const JOB_RETENTION_MS = 24 * 60 * 60 * 1000;
const EXEC_RESPONSE_MAX_BYTES = MODEL_TEXT_PAYLOAD_BYTES; // per stream; bulk output belongs in durable files/jobs, not model context
const JOB_MAX_LOG_BYTES = Math.max(16 * 1024 * 1024, Number(process.env.MCPX_JOB_MAX_LOG_BYTES) || 256 * 1024 * 1024);
const JOBS_DIR = process.env.MCPX_JOB_DIR || path.join(__dirname, 'jobs');
fs.mkdirSync(JOBS_DIR, { recursive: true, mode: 0o700 });
try { fs.chmodSync(JOBS_DIR, 0o700); } catch (_) {}

function jobPaths(id) {
  const dir = path.join(JOBS_DIR, id);
  return {
    dir,
    meta: path.join(dir, 'meta.json'),
    command: path.join(dir, 'command.zsh'),
    runner: path.join(dir, 'runner.zsh'),
    stdout: path.join(dir, 'stdout.log'),
    stderr: path.join(dir, 'stderr.log'),
    exit: path.join(dir, 'exit.json'),
  };
}

function atomicWriteJson(file, value) {
  const tmp = `${file}.tmp.${process.pid}.${crypto.randomUUID()}`;
  fs.writeFileSync(tmp, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
  fs.renameSync(tmp, file);
}

function readJson(file) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (_) {
    return null;
  }
}

function persistJob(job) {
  if (!job.persisted) return;
  atomicWriteJson(job.paths.meta, {
    id: job.id,
    command: job.command,
    cwd: job.cwd,
    pid: job.pid,
    kind: job.kind || 'background',
    status: job.status,
    exitCode: job.exitCode,
    signal: job.signal,
    startedAt: job.startedAt,
    endedAt: job.endedAt,
    endedAtMs: job.endedAtMs,
    primaryExitCode: job.primaryExitCode ?? null,
    primaryExitSignal: job.primaryExitSignal ?? null,
    primaryEndedAt: job.primaryEndedAt ?? null,
    terminationRequested: job.terminationRequested || null,
    timedOut: !!job.timedOut,
    timeoutAtMs: job.timeoutAtMs ?? null,
    keepStdinOpen: !!job.keepStdinOpen,
    visible: !!job.registered,
    operationId: job.operationId || null,
  });
  persistRecoveryJob(job);
}

function registerJob(job) {
  job.registered = true;
  commandJobs.set(job.id, job);
  persistJob(job);
}

function isJobProcessAlive(job) {
  if (!job.pid) return false;
  try {
    // The job PID is also its process-group ID because spawn uses detached:true.
    // Track the whole group so shell-level `&`/nohup descendants cannot outlive
    // the job merely because the wrapper shell exited.
    process.kill(-job.pid, 0);
    return true;
  } catch (err) {
    if (err.code === 'EPERM') return true;
    if (err.code === 'ESRCH') return false;
    return false;
  }
}

function refreshJob(job) {
  if (job.status !== 'running') return job;
  const groupAlive = isJobProcessAlive(job);
  if (groupAlive) return job;

  if (job.timeoutTimer) { clearTimeout(job.timeoutTimer); job.timeoutTimer = null; }
  if (job.logGuardTimer) { clearInterval(job.logGuardTimer); job.logGuardTimer = null; }
  // lifecycleTimer may be the callback currently invoking refreshJob; clearing it is safe.
  if (job.lifecycleTimer) { clearInterval(job.lifecycleTimer); job.lifecycleTimer = null; }

  const exitRecord = readJson(job.paths.exit);
  if (exitRecord && Number.isFinite(Number(exitRecord.exitCode))) {
    job.exitCode = Number(exitRecord.exitCode);
    job.primaryExitCode = job.primaryExitCode ?? job.exitCode;
    job.primaryEndedAt = job.primaryEndedAt ?? exitRecord.endedAt ?? null;
    job.signal = null;
    job.status = job.timedOut ? 'timed_out' : (job.terminationRequested ? 'terminated' : (job.exitCode === 0 ? 'exited' : 'failed'));
    job.endedAt = new Date().toISOString();
    job.endedAtMs = Date.now();
    persistJob(job);
    return job;
  }

  if (job.timedOut) job.status = 'timed_out';
  else if (job.terminationRequested) job.status = 'terminated';
  else job.status = 'exited_unknown';
  job.signal = job.terminationRequested || null;
  job.endedAt = job.endedAt || new Date().toISOString();
  job.endedAtMs = job.endedAtMs || Date.now();
  persistJob(job);
  return job;
}

function outputLength(file) {
  try { return fs.statSync(file).size; } catch (_) { return 0; }
}

function readOutputChunk(file, requestedOffset, maxBytes) {
  const size = outputLength(file);
  const base = Math.max(0, size - MAX_JOB_BUFFER_BYTES);
  const start = Math.min(size, Math.max(Number(requestedOffset) || 0, base));
  const length = Math.min(maxBytes, Math.max(0, size - start));
  if (length <= 0) return { size, base, start, next: start, text: '' };
  const fd = fs.openSync(file, 'r');
  try {
    const buf = Buffer.alloc(length);
    const n = fs.readSync(fd, buf, 0, length, start);
    return { size, base, start, next: start + n, text: buf.subarray(0, n).toString('utf8') };
  } finally {
    fs.closeSync(fd);
  }
}

function readOutputPrefix(file, maxBytes) {
  const size = outputLength(file);
  const length = Math.min(size, maxBytes);
  if (length <= 0) return { size, next: 0, text: '' };
  const fd = fs.openSync(file, 'r');
  try {
    const buf = Buffer.alloc(length);
    const n = fs.readSync(fd, buf, 0, length, 0);
    return { size, next: n, text: buf.subarray(0, n).toString('utf8') };
  } finally {
    fs.closeSync(fd);
  }
}

function commandJobSummary(job) {
  refreshJob(job);
  return {
    job_id: job.id,
    command: Buffer.byteLength(job.command || '', 'utf8') <= 2048 ? job.command : `${String(job.command || '').slice(0, 2048)}…`,
    command_bytes: Buffer.byteLength(job.command || '', 'utf8'),
    command_sha256: crypto.createHash('sha256').update(job.command || '').digest('hex'),
    command_truncated: Buffer.byteLength(job.command || '', 'utf8') > 2048,
    command_file: job.paths.script,
    cwd: job.cwd,
    pid: job.pid,
    status: job.status,
    exit_code: job.exitCode,
    signal: job.signal,
    primary_exit_code: job.primaryExitCode ?? null,
    primary_signal: job.primaryExitSignal ?? null,
    primary_ended_at: job.primaryEndedAt ?? null,
    started_at: job.startedAt,
    ended_at: job.endedAt,
    stdout_length: outputLength(job.paths.stdout),
    stderr_length: outputLength(job.paths.stderr),
    stdin_available: !!(job.child?.stdin && !job.child.stdin.destroyed),
    persistent: true,
    timeout_at: job.timeoutAtMs ? new Date(job.timeoutAtMs).toISOString() : null,
  };
}

function commandJobSnapshot(job, args = {}) {
  refreshJob(job);
  const maxBytes = Math.min(Math.max(Number(args.max_chars) || 20000, 1000), MODEL_TEXT_PAYLOAD_BYTES);
  const out = readOutputChunk(job.paths.stdout, args.stdout_offset, maxBytes);
  const err = readOutputChunk(job.paths.stderr, args.stderr_offset, maxBytes);
  return {
    ...commandJobSummary(job),
    stdout_offset: out.start,
    stdout_next_offset: out.next,
    stdout: out.text,
    stdout_truncated_before_offset: (Number(args.stdout_offset) || 0) < out.base,
    stderr_offset: err.start,
    stderr_next_offset: err.next,
    stderr: err.text,
    stderr_truncated_before_offset: (Number(args.stderr_offset) || 0) < err.base,
  };
}

function shellQuote(value) {
  return `'${String(value).replace(/'/g, `'\\''`)}'`;
}


const VERIFIED_GATE_BIN = process.env.MCPX_VERIFIED_GATE_BIN || path.join(__dirname, 'bin', 'mcpx-verified-gate');
function verifiedRelativePath(raw,label){
  if(typeof raw!=='string'||!raw.trim())throw new Error(`${label} must be a non-empty project-relative path`);
  if(path.isAbsolute(raw))throw new Error(`${label} must be project-relative, not absolute`);
  const rel=path.normalize(raw).replace(/\\/g,'/');
  if(rel==='.'||rel==='..'||rel.startsWith('../')||rel.includes('/../'))throw new Error(`${label} escapes the workspace: ${raw}`);
  return rel.replace(/^\.\//,'');
}
function verifiedPipelineName(raw){
  if(typeof raw!=='string'||raw.length<1||raw.length>160||!/^[A-Za-z0-9._:-]+$/.test(raw))throw new Error('pipeline must be a 1-160 character token using A-Z a-z 0-9 . _ : -');
  return raw;
}
function workspaceMutableAllows(manifest,rel){
  const target=path.normalize(rel).replace(/\\/g,'/');
  return (manifest.mutable_paths||[]).some((raw)=>{const m=path.normalize(String(raw)).replace(/\\/g,'/').replace(/\/$/,'');return target===m||target.startsWith(`${m}/`);});
}
function runningJobsForWorkspace(workspaceId){
  const wp=workspacePaths(workspaceId),want=path.resolve(wp.work),rows=[];
  for(const job of commandJobs.values()){
    try{refreshJob(job);}catch(_){}
    if(job.status==='running'&&path.resolve(job.cwd||'')===want)rows.push(commandJobSummary(job));
  }
  return rows;
}

workspaceControl = new WorkspaceControlPlane({
  workspaceRoot: WORKSPACE_ROOT,
  recoveryDir: RECOVERY_DIR,
  serverVersion: MCPX_SERVER_VERSION,
  workspacePaths,
  readJson,
  writeJson: durableWriteJson,
  workspaceChanged,
  runningJobsForWorkspace,
});
try { workspaceControl.bootstrap(); } catch (err) { console.error('[workspace control bootstrap]', err); }

function workspaceControlErrorResult(err, fallbackState='workspace_control_error') {
  if (err instanceof WorkspaceControlError) {
    return { isError:true, content:[{type:'text',text:`${err.code}: ${err.message}`}], structuredContent:{state:err.code,...(err.extra||{}),text:err.message} };
  }
  return { isError:true, content:[{type:'text',text:`Workspace control error: ${err.message || err}`}], structuredContent:{state:fallbackState,text:String(err.message || err)} };
}

function requireStateArg(args,key,label=key){
  if(typeof args?.[key]!=='string'||!args[key].trim())throw new WorkspaceControlError('STALE_CLIENT_SCHEMA_REFRESH_REQUIRED',`This MCPX release requires ${label}. Refresh/reload the ChatGPT MCP app, call reconcile_project_state, and retry with current state-bound inputs.`);
  return args[key];
}

function rejectRawWorkspacePath(fullPath, label='path') {
  if (workspaceControl && workspaceControl.isWorkspaceRootPath(fullPath)) {
    throw new WorkspaceControlError('RAW_WORKSPACE_PATH_FORBIDDEN', `${label} resolves inside MCPX workspace storage. Use a state-bound workspace handle instead.`);
  }
}

async function resolveMutationTarget(args, reason) {
  if (args.workspace_handle) {
    const pre=workspaceControl.validateWorkspaceHandle(args.workspace_handle,['open']);
    const releaseProject=await workspaceControl.acquireProjectLock(pre.record.project_path);
    try{
      const resolved = workspaceControl.resolveWorkspaceRelative(args.workspace_handle, args.path, ['open'], true);
      const consumed = await workspaceControl.consumeWorkspaceHandle(args.workspace_handle, ['open'], reason);
      return { fullPath:resolved.fullPath, workspace_id:resolved.record.workspace_id, workspace_handle:consumed.next_handle, releaseProject };
    }catch(err){await releaseProject();throw err;}
  }
  const fullPath = resolvePath(args.path);
  rejectRawWorkspacePath(fullPath, 'path');
  return { fullPath, workspace_id:null, workspace_handle:null, releaseProject:null };
}

async function consumeWorkspaceAction(handle, reason, allowedStates=['open']) {
  if (!handle) throw new WorkspaceControlError('STALE_CLIENT_SCHEMA_REFRESH_REQUIRED', 'This MCPX release requires a state-bound workspace_handle. Refresh the ChatGPT MCP app and reconcile project state before retrying.');
  const consumed = await workspaceControl.consumeWorkspaceHandle(handle, allowedStates, reason);
  const wp = workspacePaths(consumed.record.workspace_id);
  const manifest = readJson(wp.manifest);
  if (allowedStates.includes('open') && !manifest) throw new WorkspaceControlError('WORKSPACE_PHYSICAL_STATE_MISSING', `Workspace ${consumed.record.workspace_id} has no physical manifest.`);
  return {...consumed, wp, manifest};
}

function ensureWorkspaceIdle(workspaceId, label='workspace') {
  const running = runningJobsForWorkspace(workspaceId);
  if (running.length) throw new WorkspaceControlError('WORKSPACE_BUSY', `${label} ${workspaceId} has ${running.length} running MCPX job(s).`, {workspace_id:workspaceId,running_jobs:running});
}

function readReadyBuild(workspaceId, gateRel) {
  const gate = readJson(path.join(workspacePaths(workspaceId).work, gateRel));
  if (!gate || gate.state !== 'READY' || !gate.build_id) throw new WorkspaceControlError('READY_BUILD_REQUIRED', `Candidate READY gate is missing/invalid at ${gateRel}.`, {workspace_id:workspaceId,gate_path:gateRel});
  return gate;
}

function verifiedWorkspaceSandboxCommand(manifest, command, network){
  if (!COMMAND_EXECUTION_ENABLED) throw new WorkspaceControlError('COMMAND_EXECUTION_DISABLED', 'Command execution is disabled. Set MCPX_ENABLE_COMMAND_EXECUTION=1 only in a trusted deployment.');
  const wp=workspacePaths(manifest.id);
  const prefix=`export MCPX_WORKSPACE_ID=${shellQuote(manifest.id)} MCPX_CANONICAL_PROJECT=${shellQuote(manifest.project_path)}; `;
  if (WORKSPACE_SANDBOX === 'codex') {
    if (!CODEX_BIN) throw new WorkspaceControlError('WORKSPACE_SANDBOX_UNAVAILABLE', 'MCPX_WORKSPACE_SANDBOX=codex but no codex executable was found. Set MCPX_CODEX_BIN.');
    const profileArgs=['-c','permissions.mcpx_agent.extends=":workspace"'];
    if(network)profileArgs.push('-c','permissions.mcpx_agent.network.enabled=true');
    const argv=[CODEX_BIN,'sandbox',...profileArgs,'-P',CODEX_PROFILE,'-C',wp.work,SHELL_BIN,'-c',command];
    return `${prefix}exec ${argv.map(shellQuote).join(' ')}`;
  }
  if (WORKSPACE_SANDBOX !== 'none') throw new WorkspaceControlError('WORKSPACE_SANDBOX_INVALID', `Unsupported MCPX_WORKSPACE_SANDBOX=${WORKSPACE_SANDBOX}. Supported values: none, codex.`);
  return `${prefix}exec ${shellQuote(SHELL_BIN)} -lc ${shellQuote(command)}`;
}
function verifiedTimeoutMs(raw){return raw===0?null:Math.min(Math.max(Number(raw)||24*60*60*1000,60*1000),7*24*60*60*1000);}

function removeJobFiles(job) {
  try { fs.rmSync(job.paths.dir, { recursive: true, force: true }); } catch (_) {}
}

function killJobGroup(job, signal = 'SIGTERM') {
  try {
    if (job.pid) process.kill(-job.pid, signal);
    else if (job.child) job.child.kill(signal);
  } catch (err) {
    if (err.code !== 'ESRCH') throw err;
  }
}

function armJobLifecycleMonitor(job) {
  if (job.status !== 'running') return;
  if (job.lifecycleTimer) clearInterval(job.lifecycleTimer);
  job.lifecycleTimer = setInterval(() => {
    refreshJob(job);
    if (job.status !== 'running') {
      clearInterval(job.lifecycleTimer);
      job.lifecycleTimer = null;
    }
  }, 250);
  job.lifecycleTimer.unref();
}

function armJobTimeout(job) {
  if (!job.timeoutAtMs || job.status !== 'running') return;
  if (job.timeoutTimer) clearTimeout(job.timeoutTimer);
  const delay = Math.max(0, job.timeoutAtMs - Date.now());
  job.timeoutTimer = setTimeout(() => {
    refreshJob(job);
    if (job.status !== 'running') return;
    job.timedOut = true;
    job.terminationRequested = 'SIGTERM';
    persistJob(job);
    try { killJobGroup(job, 'SIGTERM'); } catch (_) {}
    setTimeout(() => {
      refreshJob(job);
      if (job.status === 'running') {
        job.terminationRequested = 'SIGKILL';
        persistJob(job);
        try { killJobGroup(job, 'SIGKILL'); } catch (_) {}
      }
    }, 2000).unref();
  }, delay);
  job.timeoutTimer.unref();
}

function armJobLogGuard(job) {
  if (job.status !== 'running') return;
  if (job.logGuardTimer) clearInterval(job.logGuardTimer);
  job.logGuardTimer = setInterval(() => {
    refreshJob(job);
    if (job.status !== 'running') {
      clearInterval(job.logGuardTimer);
      job.logGuardTimer = null;
      return;
    }
    const total = outputLength(job.paths.stdout) + outputLength(job.paths.stderr);
    if (total <= JOB_MAX_LOG_BYTES) return;
    try {
      fs.appendFileSync(job.paths.stderr, `\n[MCPX] stdout+stderr exceeded ${JOB_MAX_LOG_BYTES} bytes; terminating job to prevent disk exhaustion. Redirect bulk output to a file instead.\n`);
    } catch (_) {}
    job.terminationRequested = 'SIGTERM';
    persistJob(job);
    try { killJobGroup(job, 'SIGTERM'); } catch (_) {}
    clearInterval(job.logGuardTimer);
    job.logGuardTimer = null;
  }, 100);
  job.logGuardTimer.unref();
}

function spawnTrackedProcess(command, cwd, options = {}) {
  const id = options.id || crypto.randomUUID();
  const paths = jobPaths(id);
  fs.mkdirSync(paths.dir, { recursive: true, mode: 0o700 });
  fs.writeFileSync(paths.command, command, { mode: 0o700 });
  fs.writeFileSync(paths.stdout, '', { mode: 0o600 });
  fs.writeFileSync(paths.stderr, '', { mode: 0o600 });
  const runner = `#!/bin/zsh\numask 077\n/bin/zsh ${shellQuote(paths.command)}\nrc=$?\nended=$(/bin/date -u '+%Y-%m-%dT%H:%M:%SZ')\ntmp=${shellQuote(paths.exit)}.tmp.$$\n/usr/bin/printf '{"exitCode":%d,"endedAt":"%s"}\\n' "$rc" "$ended" > "$tmp"\n/bin/mv -f "$tmp" ${shellQuote(paths.exit)}\nexit "$rc"\n`;
  fs.writeFileSync(paths.runner, runner, { mode: 0o700 });

  const stdoutFd = fs.openSync(paths.stdout, 'a', 0o600);
  const stderrFd = fs.openSync(paths.stderr, 'a', 0o600);
  let child;
  try {
    child = spawn('/bin/zsh', [paths.runner], {
      cwd,
      detached: true,
      stdio: [options.keepStdinOpen ? 'pipe' : 'ignore', stdoutFd, stderrFd],
      env: {
        ...process.env,
        PATH: process.env.PATH || '/usr/bin:/bin',
      },
    });
  } finally {
    fs.closeSync(stdoutFd);
    fs.closeSync(stderrFd);
  }

  const job = {
    id,
    command,
    cwd,
    child,
    pid: child.pid || null,
    kind: options.kind || 'background',
    operationId: options.operationId || null,
    registered: !!options.register,
    persisted: true,
    status: 'running',
    exitCode: null,
    signal: null,
    startedAt: new Date().toISOString(),
    endedAt: null,
    endedAtMs: null,
    primaryExitCode: null,
    primaryExitSignal: null,
    primaryEndedAt: null,
    terminationRequested: null,
    timedOut: false,
    timeoutAtMs: options.timeoutMs ? Date.now() + options.timeoutMs : null,
    timeoutTimer: null,
    keepStdinOpen: !!options.keepStdinOpen,
    logGuardTimer: null,
    lifecycleTimer: null,
    paths,
  };
  if (job.registered) registerJob(job);
  else persistJob(job);
  armJobTimeout(job);
  armJobLogGuard(job);
  armJobLifecycleMonitor(job);

  child.on('error', (err) => {
    if (job.timeoutTimer) clearTimeout(job.timeoutTimer);
    if (job.logGuardTimer) clearInterval(job.logGuardTimer);
    if (job.lifecycleTimer) clearInterval(job.lifecycleTimer);
    try { fs.appendFileSync(paths.stderr, `${err.message}\n`); } catch (_) {}
    job.status = 'failed';
    job.endedAt = new Date().toISOString();
    job.endedAtMs = Date.now();
    persistJob(job);
  });
  child.on('exit', (code, signal) => {
    // Wrapper exit is only the primary process result. The job remains running while
    // any descendant in its owned process group is alive.
    const exitRecord = readJson(paths.exit);
    job.primaryExitCode = exitRecord && Number.isFinite(Number(exitRecord.exitCode)) ? Number(exitRecord.exitCode) : code;
    job.primaryExitSignal = signal || null;
    job.primaryEndedAt = exitRecord?.endedAt || new Date().toISOString();
    refreshJob(job);
    persistJob(job);
  });
  return job;
}

function loadPersistedJobs() {
  let entries = [];
  try { entries = fs.readdirSync(JOBS_DIR, { withFileTypes: true }); } catch (_) { return; }
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const paths = jobPaths(entry.name);
    const meta = readJson(paths.meta);
    if (!meta || meta.id !== entry.name) continue;
    const job = {
      id: meta.id,
      command: meta.command,
      cwd: meta.cwd,
      child: null,
      pid: meta.pid,
      kind: meta.kind || 'background',
      operationId: meta.operationId || null,
      registered: true,
      persisted: true,
      status: meta.status || 'running',
      exitCode: meta.exitCode ?? null,
      signal: meta.signal ?? null,
      startedAt: meta.startedAt,
      endedAt: meta.endedAt ?? null,
      endedAtMs: meta.endedAtMs ?? (meta.endedAt ? Date.parse(meta.endedAt) : null),
      primaryExitCode: meta.primaryExitCode ?? null,
      primaryExitSignal: meta.primaryExitSignal ?? null,
      primaryEndedAt: meta.primaryEndedAt ?? null,
      terminationRequested: meta.terminationRequested || null,
      timedOut: !!meta.timedOut,
      timeoutAtMs: meta.timeoutAtMs ?? null,
      timeoutTimer: null,
      keepStdinOpen: !!meta.keepStdinOpen,
      logGuardTimer: null,
      lifecycleTimer: null,
      paths,
    };
    refreshJob(job);
    commandJobs.set(job.id, job);
    persistJob(job);
    armJobTimeout(job);
    armJobLogGuard(job);
    armJobLifecycleMonitor(job);
  }
}

loadPersistedJobs();
backfillRecoveryOperations();

setInterval(() => {
  const cutoff = Date.now() - JOB_RETENTION_MS;
  for (const [id, job] of commandJobs) {
    refreshJob(job);
    if (job.endedAtMs && job.endedAtMs < cutoff) {
      commandJobs.delete(id);
      removeJobFiles(job);
    }
  }
}, 60 * 60 * 1000).unref();

const TOOLS = [
  {
    name: 'read_file',
    description: 'Read text from a file. Inline reads are capped at 64 KiB; use offset/limit or local processing for larger files. Ranged reads are streamed and response-bounded.',
    inputSchema: {
      type: 'object',
      properties: {
        path: { type: 'string', description: 'Path to the file to read' },
        offset: { type: 'number', description: 'Optional line offset to start reading from (1-indexed)' },
        limit: { type: 'number', description: 'Optional maximum number of lines to read' },
      },
      required: ['path'],
    },
  },
  {
    name: 'read_pdf_text',
    description: 'Extract text locally from a PDF using Poppler pdftotext. Optional 1-indexed start_page/end_page restrict extraction; PDF bytes are never sent through MCP as UTF-8.',
    inputSchema: {
      type: 'object',
      properties: {
        path: { type: 'string', description: 'Path to PDF' },
        start_page: { type: 'number', description: 'Optional first page (1-indexed)' },
        end_page: { type: 'number', description: 'Optional last page (1-indexed)' },
        layout: { type: 'boolean', description: 'Preserve approximate visual layout (default false)' },
      },
      required: ['path'],
    },
  },
  {
    name: 'get_result_info',
    description: 'Inspect metadata for a durable MCPX result preserved because its complete serialized response exceeded a configured model-facing or transport response budget.',
    inputSchema: { type: 'object', properties: { result_id: { type: 'string' } }, required: ['result_id'] },
  },
  {
    name: 'read_result_chunk',
    description: 'Read a byte-range chunk from a preserved MCPX result. format=text returns extracted text when available; format=json returns the exact preserved JSON-RPC response.',
    inputSchema: {
      type: 'object',
      properties: {
        result_id: { type: 'string' },
        format: { type: 'string', enum: ['text', 'json'], description: 'Default text' },
        offset_bytes: { type: 'number', description: 'Byte offset, default 0' },
        limit_bytes: { type: 'number', description: 'Bytes to read, default/max 65536 (64 KiB)' },
      },
      required: ['result_id'],
    },
  },
  {
    name: 'write_file',
    description: 'Transactionally create or replace a UTF-8 text file. For isolated-workspace mutation, provide the current workspace_handle and a project-relative path; raw paths inside MCPX workspace storage are refused.',
    inputSchema: {
      type: 'object',
      properties: {
        path: { type: 'string', description: 'Path to the file to write' },
        content: { type: 'string', description: 'Content to write into the file' },
        mode: { type: 'string', enum: ['create','replace','upsert'], description: 'Optional. Missing file defaults to create; existing file requires replace/upsert plus expected_sha256.' },
        expected_sha256: { type: 'string', description: 'Required when replacing an existing regular file; obtain from read_file or get_file_info.' },
        workspace_handle: { type: 'string', description: 'Optional current state-bound workspace handle. When supplied, path must be project-relative inside that workspace.' },
      },
      required: ['path', 'content'],
    },
  },
  {
    name: 'edit_file',
    description: 'Transactionally replace a UTF-8 text snippet. For isolated-workspace mutation, provide the current workspace_handle and a project-relative path; raw workspace-storage paths are refused.',
    inputSchema: {
      type: 'object',
      properties: {
        path: { type: 'string', description: 'Path to the file to edit' },
        target: { type: 'string', description: 'The exact string snippet to replace' },
        replacement: { type: 'string', description: 'The replacement text' },
        expected_sha256: { type: 'string', description: 'Optional caller-side version precondition; if supplied it must match before editing.' },
        workspace_handle: { type: 'string', description: 'Optional current state-bound workspace handle. When supplied, path must be project-relative inside that workspace.' },
      },
      required: ['path', 'target', 'replacement'],
    },
  },
  {
    name: 'list_directory',
    description: 'List a bounded page of files/directories with size and modification date. Defaults to 200 entries; use next_cursor as cursor to continue without offset-shift duplicates.',
    inputSchema: {
      type: 'object',
      properties: {
        path: { type: 'string', description: 'Directory path to list (defaults to the Research root)' },
        cursor: { type: 'string', description: 'Optional continuation cursor from next_cursor; returns names strictly after that cursor.' },
        limit: { type: 'number', description: 'Maximum entries to return (default 200, max 1000)' },
      },
    },
  },
  {
    name: 'get_file_info',
    description: 'Retrieve metadata and stats for a file or directory (size, created/modified time, is directory, permissions).',
    inputSchema: {
      type: 'object',
      properties: {
        path: { type: 'string', description: 'Path to inspect' },
      },
      required: ['path'],
    },
  },
  {
    name: 'delete_file',
    description: 'Delete a path. For isolated-workspace mutation, provide the current workspace_handle and a project-relative path; raw workspace-storage paths are refused.',
    inputSchema: {
      type: 'object',
      properties: {
        path: { type: 'string', description: 'Path to the file or directory to delete' },
        recursive: { type: 'boolean', description: 'If true, recursively delete directory contents' },
        expected_sha256: { type: 'string', description: 'Required for deleting an existing regular file; obtain from read_file/get_file_info.' },
        workspace_handle: { type: 'string', description: 'Optional current state-bound workspace handle. When supplied, path must be project-relative inside that workspace.' },
      },
      required: ['path'],
    },
  },
  {
    name: 'search_files',
    description: 'Search for files matching a keyword or pattern in file names across directories.',
    inputSchema: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Search term or pattern in filename' },
        path: { type: 'string', description: 'Directory to search within (defaults to Research root)' },
        max_depth: { type: 'number', description: 'Maximum directory depth to search (default 5)' },
      },
      required: ['query'],
    },
  },
  {
    name: 'grep_search',
    description: 'Search for text matches within files in a directory using bounded asynchronous scanning. Large/binary files are skipped and results are capped.',
    inputSchema: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Text to search for inside files' },
        path: { type: 'string', description: 'Directory to search within (defaults to Research root)' },
        case_sensitive: { type: 'boolean', description: 'Case sensitive match (default false)' },
      },
      required: ['query'],
    },
  },
  {
    name: 'reconcile_project_state',
    description: 'Use at the start of a new turn/continuation of research or workspace work, after hidden/skipped results, or whenever execution/work state is uncertain. Recover durable MCPX work-registry/execution state for the requested canonical scope: open/suspended work packets intersecting that scope, bounded recent completed-work history, project tokens, and state-bound workspace handles. `open` means unfinished work, not the latest scientific result; use canonical project artifacts/results to determine research conclusions.',
    inputSchema: { type:'object', properties:{ project_path:{type:'string',description:'Optional canonical project path. Omit to list registered projects ordered by recent state.'}, detail:{type:'boolean',description:'If true, compute current changed-path details for open workspaces.'} } },
  },
  {
    name: 'create_agent_workspace',
    description: 'Create an isolated workspace from current canonical state. Requires a project_state_token from reconcile_project_state. Canonical mutable-path overlap is checked atomically across parent/child project roots; overlap is refused unless explicitly declared as an independent audit/branch.',
    inputSchema: { type:'object', properties:{ project_path:{type:'string'}, project_state_token:{type:'string'}, mutable_paths:{type:'array',items:{type:'string'}}, name:{type:'string'}, branch_of_handle:{type:'string'}, branch_role:{type:'string',enum:['independent_audit','independent_branch']}, branch_reason:{type:'string'} }, required:['project_path','project_state_token','mutable_paths'] },
  },
  {
    name: 'agent_workspace_info',
    description: 'Read-only forensic lookup for a workspace UUID. Returns open state with a fresh state-bound handle, or durable retired/suspended tombstone/history instead of treating old UUIDs as unknown.',
    inputSchema: { type:'object', properties:{ workspace_id:{type:'string'} }, required:['workspace_id'] },
  },
  {
    name: 'start_workspace_command',
    description: 'Run a standalone persistent command in an isolated workspace. Requires a fresh workspace_handle; remembered/raw workspace IDs are not accepted. Use verified producer/consumer actions for dependent pipelines.',
    inputSchema: { type:'object', properties:{ workspace_handle:{type:'string'}, command:{type:'string'}, network:{type:'boolean'}, timeout_ms:{type:'number'} }, required:['workspace_handle','command'] },
  },
  {
    name: 'start_verified_workspace_producer',
    description: 'Launch a fail-closed verified producer in an isolated workspace using a fresh workspace_handle. Publishes an immutable READY generation/build only after exit 0, passed result validation, and complete artifact hashing.',
    inputSchema: {type:'object',properties:{workspace_handle:{type:'string'},pipeline:{type:'string'},gate_path:{type:'string'},result_json:{type:'string'},artifacts:{type:'array',minItems:1,items:{type:'string'}},command:{type:'string'},expectations:{type:'object',additionalProperties:true},metadata:{type:'object',additionalProperties:true},network:{type:'boolean'},timeout_ms:{type:'number'}},required:['workspace_handle','pipeline','gate_path','result_json','artifacts','command']},
  },
  {
    name: 'start_verified_workspace_consumer',
    description: 'Launch an independent validator/audit using fresh consumer and candidate workspace handles. Requires the exact candidate READY build_id and re-verifies all manifested hashes before and after the audit.',
    inputSchema: {type:'object',properties:{workspace_handle:{type:'string'},candidate_workspace_handle:{type:'string'},candidate_build_id:{type:'string'},pipeline:{type:'string'},gate_path:{type:'string'},required_artifacts:{type:'array',items:{type:'string'}},command:{type:'string'},network:{type:'boolean'},timeout_ms:{type:'number'}},required:['workspace_handle','candidate_workspace_handle','candidate_build_id','pipeline','gate_path','command']},
  },
  {
    name: 'integrate_agent_workspace',
    description: 'Reconcile BASE/CURRENT/PROPOSED into canonical using a fresh workspace_handle. Refuses while the workspace has a running job; successful integration permanently retires the mutable generation from further side effects.',
    inputSchema: { type:'object', properties:{ workspace_handle:{type:'string'}, dry_run:{type:'boolean'} }, required:['workspace_handle'] },
  },
  {
    name: 'remove_agent_workspace',
    description: 'Retire an isolated workspace using a fresh workspace_handle. Dirty force retirement preserves/quarantines divergent state before physical removal; history is never erased.',
    inputSchema: { type:'object', properties:{ workspace_handle:{type:'string'}, force:{type:'boolean'}, reason:{type:'string'} }, required:['workspace_handle'] },
  },
  {
    name: 'resume_workspace',
    description: 'Restore a suspended cold workspace from its verified archive. Requires a fresh suspended workspace_handle and current project_state_token; returns a new open handle.',
    inputSchema: { type:'object', properties:{ workspace_handle:{type:'string'}, project_state_token:{type:'string'} }, required:['workspace_handle','project_state_token'] },
  },
  {
    name: 'start_command',
    description: 'Start a potentially long-running shell command outside isolated workspace storage. Raw cwd paths beneath MCPX workspace storage are refused; use start_workspace_command with a fresh handle for workspace work.',
    inputSchema: {
      type: 'object',
      properties: {
        command: { type: 'string', description: 'Shell command to start' },
        cwd: { type: 'string', description: 'Working directory (defaults to Research root)' },
        keep_stdin_open: { type: 'boolean', description: 'Keep a writable stdin pipe open for send_command_input (default false). Noninteractive jobs should leave this false.' },
        timeout_ms: { type: 'number', description: 'Maximum runtime in milliseconds. Defaults to 24 hours; 0 disables the runtime limit. Maximum explicit limit is 7 days.' },
      },
      required: ['command'],
    },
  },
  {
    name: 'command_status',
    description: 'Get status and incremental stdout/stderr for a background command job.',
    inputSchema: {
      type: 'object',
      properties: {
        job_id: { type: 'string', description: 'Job ID returned by start_command' },
        stdout_offset: { type: 'number', description: 'Absolute stdout byte offset to read from (default 0)' },
        stderr_offset: { type: 'number', description: 'Absolute stderr byte offset to read from (default 0)' },
        max_chars: { type: 'number', description: 'Maximum bytes returned per stream (default 20000, max 65536)' },
      },
      required: ['job_id'],
    },
  },
  {
    name: 'send_command_input',
    description: 'Send text to stdin of a running background command, optionally closing stdin afterwards.',
    inputSchema: {
      type: 'object',
      properties: {
        job_id: { type: 'string', description: 'Job ID returned by start_command' },
        input: { type: 'string', description: 'Text to write to stdin' },
        close_stdin: { type: 'boolean', description: 'Close stdin after writing (default false)' },
      },
      required: ['job_id', 'input'],
    },
  },
  {
    name: 'stop_command',
    description: 'Stop a running background command and its process group.',
    inputSchema: {
      type: 'object',
      properties: {
        job_id: { type: 'string', description: 'Job ID returned by start_command' },
        force: { type: 'boolean', description: 'Use SIGKILL instead of SIGTERM (default false)' },
      },
      required: ['job_id'],
    },
  },
  {
    name: 'list_command_jobs',
    description: 'List recent background command jobs as a bounded reverse-chronological page. Defaults to 50 jobs; use next_cursor as cursor to continue safely while new jobs are inserted.',
    inputSchema: {
      type: 'object',
      properties: {
        cursor: { type: 'string', description: 'Optional continuation cursor from next_cursor; stable against new jobs inserted at the front.' },
        limit: { type: 'number', description: 'Maximum jobs to return (default 50, max 200)' },
      },
    },
  },
  {
    name: 'execute_command',
    description: 'Execute a short command. For isolated workspace execution, provide workspace_handle and an optional project-relative cwd. Raw cwd paths beneath MCPX workspace storage are refused.',
    inputSchema: {
      type: 'object',
      properties: {
        command: { type: 'string', description: 'Shell command to execute' },
        cwd: { type: 'string', description: 'Working directory. With workspace_handle this must be project-relative inside the workspace; otherwise defaults to Research root.' },
        workspace_handle: { type: 'string', description: 'Optional current state-bound workspace handle for short isolated-workspace execution.' },
        timeout_ms: { type: 'number', description: 'Hard command execution timeout in milliseconds (default: 120000). If still running after 20 seconds, the command is promoted to a tracked job.' },
      },
      required: ['command'],
    },
  },
];

for (const tool of TOOLS) {
  tool.outputSchema = { type:'object', additionalProperties:true };
  if (!SIDE_EFFECTING_TOOLS.has(tool.name)) continue;
  tool.inputSchema = tool.inputSchema || { type:'object', properties:{} };
  tool.inputSchema.properties = tool.inputSchema.properties || {};
  tool.inputSchema.properties.operation_id = { type:'string', minLength:8, maxLength:160, description:'Stable idempotency token for this logical side effect. Generate a new UUID/token for a new action; reuse the identical value only when retrying the same tool call after a lost/uncertain response.' };
  tool.inputSchema.required = [...new Set([...(tool.inputSchema.required || []), 'operation_id'])];
}

function createMcpServer() {
  const server = new Server(
    { name: 'mcpx', version: MCPX_SERVER_VERSION },
    { capabilities: { tools: {} } }
  );

  server.setRequestHandler('tools/list', async () => {
  const tools = COMMAND_EXECUTION_ENABLED ? TOOLS : TOOLS.filter((tool) => !COMMAND_START_TOOLS.has(tool.name));
  return { tools };
});

  server.setRequestHandler('tools/call', async (request, ctx) => {
  const { name, arguments: rawArgs } = request.params;
  const args = rawArgs || {};
  if (COMMAND_START_TOOLS.has(name) && !COMMAND_EXECUTION_ENABLED) {
    return { isError:true, content:[{type:'text',text:'Command execution is disabled by default. Set MCPX_ENABLE_COMMAND_EXECUTION=1 only in a trusted deployment.'}], structuredContent:{state:'COMMAND_EXECUTION_DISABLED'} };
  }
  let operation = null;
  try {
    operation = await beginSideEffectOperation(name, args);
    if (operation?.early) return operation.early;
  } catch (err) {
    return operationResultError(`Operation setup failed: ${err.message}`, { tool:name, state:'operation_setup_failed' });
  }

  const runTool = async () => {
  try {
    switch (name) {
      case 'read_file': {
        const fullPath = resolvePath(args.path);
        const MAX_READ_BYTES = MODEL_TEXT_PAYLOAD_BYTES; // model-facing inline bound; use ranged reads/local processing for larger text
        let stat;
        try { stat = await fs.promises.stat(fullPath); }
        catch (err) { return { isError: true, content: [{ type: 'text', text: `File not found or unreadable: ${fullPath} (${err.message})` }] }; }
        if (stat.isDirectory()) return { isError: true, content: [{ type: 'text', text: `Path is a directory: ${fullPath}. Use list_directory instead.` }] };
        const classification = await classifyTextFile(fullPath, stat);
        if (!classification.text) {
          const hint = classification.kind === 'pdf'
            ? 'Use read_pdf_text for local PDF text extraction.'
            : 'Process this file locally with an appropriate parser/script and return derived text/results.';
          return {
            isError: true,
            content: [{ type: 'text', text: `read_file is a UTF-8 text tool. ${fullPath} is ${classification.reason}. ${hint}` }],
            structuredContent: { path: fullPath, bytes: stat.size, kind: classification.kind, reason: classification.reason },
          };
        }
        const sha256 = await sha256File(fullPath);
        const version = { path: fullPath, sha256, bytes: stat.size, mtime: stat.mtime.toISOString(), kind: 'text' };
        const hasRange = args.offset !== undefined || args.limit !== undefined;
        if (!hasRange) {
          if (stat.size > MAX_READ_BYTES) return { isError: true, content: [{ type: 'text', text: `Text file is ${stat.size} bytes, above the ${MAX_READ_BYTES}-byte model-facing inline limit. Use offset/limit or process it locally.` }], structuredContent: version };
          const raw = await fs.promises.readFile(fullPath);
          let content;
          try { content = new TextDecoder('utf-8', { fatal: true }).decode(raw); }
          catch (_) { return { isError: true, content: [{ type: 'text', text: `File contains invalid UTF-8 beyond the initial sample: ${fullPath}` }], structuredContent: version }; }
          return { content: [{ type: 'text', text: content }], structuredContent: version };
        }
        const startLine = Math.max(1, Math.floor(Number(args.offset) || 1));
        const requestedLimit = args.limit === undefined ? Number.POSITIVE_INFINITY : Math.max(0, Math.floor(Number(args.limit) || 0));
        const input = fs.createReadStream(fullPath, { encoding: 'utf8' });
        const rl = readline.createInterface({ input, crlfDelay: Infinity });
        const lines = []; let lineNo = 0; let selected = 0; let bytes = 0; let truncated = false;
        try {
          for await (const line of rl) {
            lineNo += 1; if (lineNo < startLine) continue; if (selected >= requestedLimit) break;
            const addition = `${line}${lines.length ? '\n' : ''}`;
            const additionBytes = Buffer.byteLength(addition, 'utf8');
            if (bytes + additionBytes > MAX_READ_BYTES) {
              if (selected === 0) return { isError:true, content:[{type:'text',text:`Line ${lineNo} is ${additionBytes} UTF-8 bytes, above the ${MAX_READ_BYTES}-byte model-facing line limit. Process this file locally or transform it before reading through MCPX.`}], structuredContent:{...version,start_line:startLine,oversize_line:lineNo,oversize_line_bytes:additionBytes} };
              truncated = true; break;
            }
            lines.push(line); bytes += additionBytes; selected += 1;
          }
        } finally { rl.close(); input.destroy(); }
        const blocks = [{ type: 'text', text: lines.join('\n') }];
        if (truncated) blocks.push({ type: 'text', text: `[MCPX note: ranged read stopped at the ${MAX_READ_BYTES}-byte model-facing per-call bound. Continue from approximately line ${startLine + selected}.]` });
        return { content: blocks, structuredContent: { ...version, start_line: startLine, lines_returned: selected, truncated } };
      }

      case 'read_pdf_text': {
        const fullPath = resolvePath(args.path);
        let stat;
        try { stat = await fs.promises.stat(fullPath); } catch (err) { return { isError: true, content: [{ type: 'text', text: `PDF not found or unreadable: ${fullPath} (${err.message})` }] }; }
        const classification = await classifyTextFile(fullPath, stat);
        if (classification.kind !== 'pdf') return { isError: true, content: [{ type: 'text', text: `Not a PDF: ${fullPath} (${classification.reason})` }] };
        if (!PDFINFO_BIN || !PDFTOTEXT_BIN) return { isError:true, content:[{type:'text',text:'PDF extraction requires Poppler (pdfinfo and pdftotext) on PATH, or MCPX_PDFINFO_BIN/MCPX_PDFTOTEXT_BIN.'}], structuredContent:{state:'PDF_TOOLS_UNAVAILABLE'} };
        const info = await runProcessCapture(PDFINFO_BIN, [fullPath], { maxBytes: 1024 * 1024, timeoutMs: 15000 });
        if (info.code !== 0) return { isError: true, content: [{ type: 'text', text: `pdfinfo failed (${info.code}): ${info.stderr.slice(0, 4000)}` }] };
        const pagesMatch = info.stdout.match(/^Pages:\s+(\d+)/mi);
        const totalPages = pagesMatch ? Number(pagesMatch[1]) : null;
        const startPage = Math.max(1, Math.floor(Number(args.start_page) || 1));
        const endPage = args.end_page === undefined ? (totalPages || startPage) : Math.max(startPage, Math.floor(Number(args.end_page) || startPage));
        const pdftotextArgs = [];
        if (args.layout === true) pdftotextArgs.push('-layout');
        pdftotextArgs.push('-f', String(startPage), '-l', String(endPage), fullPath, '-');
        const extracted = await runProcessCapture(PDFTOTEXT_BIN, pdftotextArgs, { maxBytes: MODEL_TEXT_PAYLOAD_BYTES, timeoutMs: 120000 });
        if (extracted.killedForSize) return { isError: true, content: [{ type: 'text', text: `PDF extraction exceeded the 64 KiB model-facing capture bound. Request a smaller page range.` }], structuredContent: { path: fullPath, pages: totalPages, start_page: startPage, end_page: endPage } };
        if (extracted.code !== 0) return { isError: true, content: [{ type: 'text', text: `pdftotext failed (${extracted.code}): ${extracted.stderr.slice(0, 4000)}` }] };
        return { content: [{ type: 'text', text: extracted.stdout }], structuredContent: { path: fullPath, source_bytes: stat.size, source_sha256: await sha256File(fullPath), pages: totalPages, start_page: startPage, end_page: endPage, extracted_text_bytes: Buffer.byteLength(extracted.stdout, 'utf8') } };
      }

      case 'get_result_info': {
        if (!/^[0-9a-f-]{36}$/i.test(String(args.result_id || ''))) return { isError: true, content: [{ type: 'text', text: 'Invalid result_id' }] };
        const meta = readJson(resultPaths(args.result_id).meta);
        if (!meta) return { isError: true, content: [{ type: 'text', text: `Unknown result_id: ${args.result_id}` }] };
        return { content: [{ type: 'text', text: JSON.stringify(meta, null, 2) }], structuredContent: meta };
      }

      case 'read_result_chunk': {
        if (!/^[0-9a-f-]{36}$/i.test(String(args.result_id || ''))) return { isError: true, content: [{ type: 'text', text: 'Invalid result_id' }] };
        const paths = resultPaths(args.result_id); const meta = readJson(paths.meta);
        if (!meta) return { isError: true, content: [{ type: 'text', text: `Unknown result_id: ${args.result_id}` }] };
        const format = args.format === 'json' ? 'json' : 'text';
        const file = format === 'text' && meta.text_file ? paths.text : paths.json;
        const st = await fs.promises.stat(file);
        const offset = Math.min(st.size, Math.max(0, Math.floor(Number(args.offset_bytes) || 0)));
        const limit = Math.min(MODEL_TEXT_PAYLOAD_BYTES, Math.max(1, Math.floor(Number(args.limit_bytes) || MODEL_TEXT_PAYLOAD_BYTES)));
        const length = Math.min(limit, st.size - offset); const fh = await fs.promises.open(file, 'r'); const buf = Buffer.alloc(length);
        try { if (length) await fh.read(buf, 0, length, offset); } finally { await fh.close(); }
        return { content: [{ type: 'text', text: buf.toString('utf8') }], structuredContent: { result_id: args.result_id, format, total_bytes: st.size, offset_bytes: offset, returned_bytes: length, next_offset_bytes: offset + length, eof: offset + length >= st.size, sha256: meta.response_sha256 } };
      }

      case 'write_file': {
        const mutation=await resolveMutationTarget(args,'write_file'); const fullPath=mutation.fullPath;
        try{
          const result=await performTransactionalWrite(fullPath,String(args.content ?? ''),{mode:args.mode || 'auto',expectedSha:args.expected_sha256 || null});
          const ws={workspace_id:mutation.workspace_id,workspace_handle:mutation.workspace_handle};
          if(result.status==='conflict') return {isError:true,content:[{type:'text',text:`Write conflict for ${fullPath}: ${result.reason}`}],structuredContent:{path:fullPath,...result,...ws}};
          if(result.status==='error') return {isError:true,content:[{type:'text',text:`Write failed for ${fullPath}: ${result.message || result.reason}`}],structuredContent:{path:fullPath,...result,...ws}};
          return {content:[{type:'text',text:`Successfully ${result.status==='created'?'created':'wrote'} ${result.bytes ?? Buffer.byteLength(String(args.content ?? ''),'utf8')} bytes at ${fullPath}`}],structuredContent:{path:fullPath,...result,...ws}};
        }finally{if(mutation.releaseProject)await mutation.releaseProject();}
      }

      case 'edit_file': {
        const mutation=await resolveMutationTarget(args,'edit_file'); const fullPath=mutation.fullPath;
        try{
          const result=await withFileMutationLock(fullPath,async()=>{
            const info=await targetRegularFileInfo(fullPath);
            if(!info.exists) return {status:'error',reason:'target_missing',message:`File not found: ${fullPath}`};
            if(info.symlink) return {status:'error',reason:'symlink',message:`Refusing to edit symbolic link: ${fullPath}`};
            if(!info.regular) return {status:'error',reason:'not_regular_file',message:`Not a regular file: ${fullPath}`};
            if(args.expected_sha256 && args.expected_sha256!==info.sha256) return {status:'conflict',reason:'version_mismatch',expected_sha256:args.expected_sha256,actual_sha256:info.sha256};
            const stat=info.stat; const MAX_EDIT_BYTES=64*1024*1024;
            if(stat.size>MAX_EDIT_BYTES) return {status:'error',reason:'too_large',message:`File is ${stat.size} bytes, above the ${MAX_EDIT_BYTES}-byte edit_file safety limit.`};
            const classification=await classifyTextFile(fullPath,stat);
            if(!classification.text) return {status:'error',reason:'not_text',message:`edit_file is UTF-8 text only: ${classification.reason}`};
            const raw=await fs.promises.readFile(fullPath); let content;
            try { content=new TextDecoder('utf-8',{fatal:true}).decode(raw); } catch (_) { return {status:'error',reason:'invalid_utf8',message:'File contains invalid UTF-8'}; }
            if(!content.includes(String(args.target))) return {status:'conflict',reason:'target_string_not_found',actual_sha256:info.sha256};
            const newContent=content.replace(String(args.target),String(args.replacement));
            const temp=await createDurableTemp(fullPath,newContent); const newSha=crypto.createHash('sha256').update(newContent,'utf8').digest('hex'); const id=crypto.randomUUID();
            const rec={id,op:'replace',target:fullPath,temp,expected_sha256:info.sha256,new_sha256:newSha,created_at:new Date().toISOString()}; durableWriteJson(txnJournalPath(id),rec);
            try {
              const helper=await runFileTxnHelper('replace',fullPath,info.sha256,temp);
              if(helper.status==='conflict'){await reconcileFileTxn(rec,{cleanup:true});return helper;}
              await fsyncDirectory(path.dirname(fullPath)); try{await fs.promises.unlink(txnJournalPath(id));}catch(_){}
              return {...helper,transaction_id:id};
            } catch(err) {
              const rr=await reconcileFileTxn(rec,{cleanup:true});
              if(rr.state==='completed') return {status:'written',recovered_after_helper_error:true,new_sha256:newSha,bytes:Buffer.byteLength(newContent,'utf8'),transaction_id:id};
              return {status:'error',reason:'helper_failure',message:err.message,recovery_state:rr.state};
            }
          });
          const ws={workspace_id:mutation.workspace_id,workspace_handle:mutation.workspace_handle};
          if(result.status==='conflict') return {isError:true,content:[{type:'text',text:`Edit conflict for ${fullPath}: ${result.reason}`}],structuredContent:{path:fullPath,...result,...ws}};
          if(result.status==='error') return {isError:true,content:[{type:'text',text:`Edit failed for ${fullPath}: ${result.message || result.reason}`}],structuredContent:{path:fullPath,...result,...ws}};
          return {content:[{type:'text',text:`Successfully replaced text in ${fullPath}`}],structuredContent:{path:fullPath,...result,...ws}};
        }finally{if(mutation.releaseProject)await mutation.releaseProject();}
      }

      case 'list_directory': {
        const fullPath = resolvePath(args.path);
        let entries;
        try { entries = await fs.promises.readdir(fullPath, { withFileTypes: true }); }
        catch (err) { return { isError: true, content: [{ type: 'text', text: `Directory not found or unreadable: ${fullPath} (${err.message})` }] }; }
        entries.sort((a,b)=>a.name < b.name ? -1 : (a.name > b.name ? 1 : 0));
        const totalEntries = entries.length;
        const cursor = typeof args.cursor === 'string' ? args.cursor : null;
        const eligible = cursor === null ? entries : entries.filter(e => e.name > cursor);
        const limit = Math.min(1000, Math.max(1, Math.floor(Number(args.limit) || 200)));
        const selected = eligible.slice(0, limit);
        const results = await Promise.all(selected.map(async (e) => {
          const entryPath = path.join(fullPath, e.name);
          let size = 0;
          let mtime = null;
          try {
            const stat = await fs.promises.lstat(entryPath);
            size = stat.size;
            mtime = stat.mtime.toISOString();
          } catch (_) {}
          return { name:e.name, isDirectory:e.isDirectory(), isFile:e.isFile(), isSymbolicLink:e.isSymbolicLink(), sizeBytes:size, modifiedAt:mtime };
        }));
        const remaining = eligible.length > selected.length;
        const nextCursor = remaining && selected.length ? selected[selected.length-1].name : null;
        const payload={path:fullPath,total_entries:totalEntries,cursor,limit,returned:results.length,truncated:remaining,next_cursor:nextCursor,items:results};
        return { content: [{ type:'text', text:JSON.stringify(payload,null,2) }] };
      }

      case 'get_file_info': {
        const fullPath = resolvePath(args.path);
        let stat;
        try { stat = await fs.promises.lstat(fullPath); }
        catch (_) { return { isError: true, content: [{ type: 'text', text: `Path not found: ${fullPath}` }] }; }
        const info = {
          path: fullPath,
          sizeBytes: stat.size,
          isDirectory: stat.isDirectory(),
          isFile: stat.isFile(),
          isSymbolicLink: stat.isSymbolicLink(),
          createdAt: stat.birthtime.toISOString(),
          modifiedAt: stat.mtime.toISOString(),
          permissions: (stat.mode & parseInt('777', 8)).toString(8),
        };
        if (stat.isSymbolicLink()) {
          try {
            const target = await fs.promises.readlink(fullPath);
            const targetPath = path.resolve(path.dirname(fullPath), target);
            info.symlinkTarget = target;
            info.symlinkTargetResolved = targetPath;
            try {
              const targetStat = await fs.promises.stat(fullPath);
              info.targetExists = true;
              info.targetIsDirectory = targetStat.isDirectory();
              info.targetIsFile = targetStat.isFile();
            } catch (_) {
              info.targetExists = false;
            }
          } catch (_) {}
        }
        if (stat.isFile() && !stat.isSymbolicLink()) {
          try { info.sha256 = await sha256File(fullPath); } catch (_) {}
        }
        return { content: [{ type: 'text', text: JSON.stringify(info, null, 2) }], structuredContent: info };
      }

      case 'delete_file': {
        const mutation=await resolveMutationTarget(args,'delete_file'); const fullPath=mutation.fullPath; const ws={workspace_id:mutation.workspace_id,workspace_handle:mutation.workspace_handle};
        try{
          let stat; try{stat=await fs.promises.lstat(fullPath);}catch(err){if(err.code==='ENOENT')return {content:[{type:'text',text:`Already absent: ${fullPath}`}],structuredContent:{path:fullPath,status:'deleted',already_absent:true,...ws}};throw err;}
          if(stat.isSymbolicLink()) { await fs.promises.unlink(fullPath); await fsyncDirectory(path.dirname(fullPath)); return {content:[{type:'text',text:`Deleted symbolic link ${fullPath}`}],structuredContent:{path:fullPath,status:'deleted',symlink:true,...ws}}; }
          if(stat.isDirectory()) { await fs.promises.rm(fullPath,{recursive:!!args.recursive,force:false}); await fsyncDirectory(path.dirname(fullPath)); return {content:[{type:'text',text:`Deleted directory ${fullPath}`}],structuredContent:{path:fullPath,status:'deleted',directory:true,...ws}}; }
          const result=await performTransactionalDelete(fullPath,args.expected_sha256 || null);
          if(result.status==='conflict') return {isError:true,content:[{type:'text',text:`Delete conflict for ${fullPath}: ${result.reason}`}],structuredContent:{path:fullPath,...result,...ws}};
          if(result.status==='error') return {isError:true,content:[{type:'text',text:`Delete failed for ${fullPath}: ${result.message || result.reason}`}],structuredContent:{path:fullPath,...result,...ws}};
          return {content:[{type:'text',text:`Successfully deleted ${fullPath}`}],structuredContent:{path:fullPath,...result,...ws}};
        }finally{if(mutation.releaseProject)await mutation.releaseProject();}
      }

      case 'search_files': {
        const rootPath = resolvePath(args.path);
        const query = String(args.query || '').toLowerCase();
        const maxDepth = Math.min(Math.max(args.max_depth === undefined ? 5 : Math.floor(Number(args.max_depth) || 0), 0), 20);
        const MAX_MATCHES = 100;
        const MAX_ENTRIES = 50000;
        const DEADLINE_MS = 10000;
        const deadline = Date.now() + DEADLINE_MS;
        const matches = [];
        let scanned = 0;
        let truncated = false;
        const stack = [{ dir: rootPath, depth: 0 }];

        while (stack.length && matches.length < MAX_MATCHES) {
          if (Date.now() > deadline || scanned >= MAX_ENTRIES) { truncated = true; break; }
          const { dir, depth } = stack.pop();
          let handle;
          try { handle = await fs.promises.opendir(dir); }
          catch (_) { continue; }
          try {
            for await (const item of handle) {
              scanned += 1;
              if (Date.now() > deadline || scanned >= MAX_ENTRIES) { truncated = true; break; }
              const fullItemPath = path.join(dir, item.name);
              if (item.name.toLowerCase().includes(query)) {
                matches.push({ path: fullItemPath, name: item.name, isDirectory: item.isDirectory() });
                if (matches.length >= MAX_MATCHES) { truncated = true; break; }
              }
              if (depth < maxDepth && item.isDirectory() && !item.name.startsWith('.') && item.name !== 'node_modules') {
                stack.push({ dir: fullItemPath, depth: depth + 1 });
              }
            }
          } catch (_) {}
        }
        const payload={query:String(args.query||''),root:rootPath,scanned_entries:scanned,returned:matches.length,truncated,items:matches};
        return {content:[{type:'text',text:JSON.stringify(payload,null,2)}]};
      }

      case 'grep_search': {
        const rootPath = resolvePath(args.path);
        const query = String(args.query || '');
        const queryCmp = args.case_sensitive ? query : query.toLowerCase();
        const caseSensitive = !!args.case_sensitive;
        const MAX_MATCHES = 50;
        const MAX_FILES = 10000;
        const MAX_FILE_BYTES = 2 * 1024 * 1024;
        const MAX_LINE_CHARS = 2000;
        const DEADLINE_MS = 10000;
        const deadline = Date.now() + DEADLINE_MS;
        const matches = [];
        let scannedFiles = 0;
        let truncated = false;
        const stack = [{ dir: rootPath, depth: 0 }];

        while (stack.length && matches.length < MAX_MATCHES) {
          if (Date.now() > deadline || scannedFiles >= MAX_FILES) { truncated = true; break; }
          const { dir, depth } = stack.pop();
          let handle;
          try { handle = await fs.promises.opendir(dir); }
          catch (_) { continue; }
          try {
            for await (const item of handle) {
              if (Date.now() > deadline || scannedFiles >= MAX_FILES || matches.length >= MAX_MATCHES) { truncated = true; break; }
              const fullItemPath = path.join(dir, item.name);
              if (item.isDirectory()) {
                if (depth < 6 && !item.name.startsWith('.') && item.name !== 'node_modules') stack.push({ dir: fullItemPath, depth: depth + 1 });
                continue;
              }
              if (!item.isFile()) continue;
              scannedFiles += 1;
              try {
                const stat = await fs.promises.stat(fullItemPath);
                if (stat.size > MAX_FILE_BYTES) continue;
                const content = await fs.promises.readFile(fullItemPath, 'utf8');
                if (content.includes(String.fromCharCode(0))) continue;
                const lines = content.split('\n');
                for (let i = 0; i < lines.length; i++) {
                  const line = lines[i];
                  const haystack = caseSensitive ? line : line.toLowerCase();
                  if (haystack.includes(queryCmp)) {
                    let excerpt = line.trim();
                    if (excerpt.length > MAX_LINE_CHARS) excerpt = `${excerpt.slice(0, MAX_LINE_CHARS)}…`;
                    matches.push({ file: fullItemPath, line: i + 1, content: excerpt });
                    if (matches.length >= MAX_MATCHES) { truncated = true; break; }
                  }
                }
              } catch (_) {}
            }
          } catch (_) {}
        }
        const payload={query,root:rootPath,case_sensitive:caseSensitive,scanned_files:scannedFiles,returned:matches.length,truncated,items:matches};
        return {content:[{type:'text',text:JSON.stringify(payload,null,2)}]};
      }

      case 'reconcile_project_state': {
        const payload=await workspaceControl.reconcileProjectState(args.project_path || null,args.detail===true);
        return {content:[{type:'text',text:JSON.stringify(payload,null,2)}],structuredContent:payload};
      }

      case 'create_agent_workspace': {
        requireStateArg(args,'project_state_token','project_state_token');
        const project=await fs.promises.realpath(resolvePath(args.project_path)); const st=await fs.promises.stat(project); if(!st.isDirectory())return {isError:true,content:[{type:'text',text:`Project is not a directory: ${project}`}]};
        const mutables=compactMutablePaths(args.mutable_paths); const release=await workspaceControl.acquireTopologyLock();
        try{
          const validated=workspaceControl.validateCreateRequest({project_path:project,project_state_token:args.project_state_token,mutable_paths:mutables,branch_of_handle:args.branch_of_handle||null,branch_role:args.branch_role||null,branch_reason:args.branch_reason||null});
          const id=args.__mcpx_logical_id || crypto.randomUUID(),wp=workspacePaths(id); await fs.promises.mkdir(wp.dir,{recursive:true,mode:0o700});await fs.promises.mkdir(wp.work,{recursive:true,mode:0o700});await fs.promises.mkdir(wp.base,{recursive:true,mode:0o700});
          try{
            await materializeWorkspaceMirror(project,wp.work,wp.base,mutables);await fs.promises.symlink(project,path.join(wp.work,'.mcpx-canonical'));
            const manifest={id,name:typeof args.name==='string'?args.name:null,project_path:project,mutable_paths:mutables,created_at:new Date().toISOString(),status:'open',server_version:MCPX_SERVER_VERSION,role:args.branch_role||'development',branch_parent_workspace_id:validated.branch_parent?.workspace_id||null};
            manifest.base_states=await collectMutableStates(wp.base,mutables);durableWriteJson(wp.manifest,manifest);
            const registered=workspaceControl.registerCreated(manifest,{branch_parent:validated.branch_parent,branch_role:args.branch_role||null,branch_reason:args.branch_reason||null});
            const payload={workspace_id:id,workspace_path:wp.work,canonical_project:project,canonical_link:path.join(wp.work,'.mcpx-canonical'),mutable_paths:mutables,status:'open',role:manifest.role,branch_parent_workspace_id:manifest.branch_parent_workspace_id,workspace_handle:registered.workspace_handle,project_state_token:registered.project_state_token};
            return {content:[{type:'text',text:`Created isolated workspace ${id} at ${wp.work}`}],structuredContent:payload};
          }catch(err){await fs.promises.rm(wp.dir,{recursive:true,force:true});throw err;}
        } finally { await release(); }
      }

      case 'agent_workspace_info': {
        if(!validateWorkspaceId(args.workspace_id))return {isError:true,content:[{type:'text',text:'Invalid workspace_id'}]};
        const rec=workspaceControl.tombstoneInfo(args.workspace_id); if(!rec)return {isError:true,content:[{type:'text',text:`Unknown workspace: ${args.workspace_id}`}],structuredContent:{state:'UNKNOWN_WORKSPACE',workspace_id:args.workspace_id}};
        const wp=workspacePaths(args.workspace_id),manifest=readJson(wp.manifest);
        if(manifest){const changed=await workspaceChanged(manifest);const payload={...manifest,status:rec.state,workspace_path:wp.work,changed_paths:changed,control_state:rec.state,revision:rec.revision,workspace_handle:rec.workspace_handle};return {content:[{type:'text',text:JSON.stringify(payload,null,2)}],structuredContent:payload};}
        return {content:[{type:'text',text:JSON.stringify(rec,null,2)}],structuredContent:rec};
      }

      case 'start_workspace_command': {
        requireStateArg(args,'workspace_handle','workspace_handle');
        const pre=workspaceControl.validateWorkspaceHandle(args.workspace_handle,['open']);const releaseProject=await workspaceControl.acquireProjectLock(pre.record.project_path);
        try{
          workspaceControl.validateWorkspaceHandle(args.workspace_handle,['open']);ensureWorkspaceIdle(pre.record.workspace_id);
          const state=await consumeWorkspaceAction(args.workspace_handle,'start_workspace_command',['open']); const {wp,manifest}=state;
          const network=args.network===true;const timeout=args.timeout_ms===0?null:Math.min(Math.max(Number(args.timeout_ms)||24*60*60*1000,60*1000),7*24*60*60*1000);const wrapped=verifiedWorkspaceSandboxCommand(manifest,args.command,network);const job=spawnTrackedProcess(wrapped,wp.work,{id:args.__mcpx_logical_id,register:true,kind:'workspace',operationId:args.operation_id,keepStdinOpen:false,timeoutMs:timeout});
          const payload={...commandJobSummary(job),workspace_id:manifest.id,workspace_path:wp.work,workspace_handle:state.next_handle,network};return {content:[{type:'text',text:JSON.stringify(payload,null,2)}],structuredContent:payload};
        }finally{await releaseProject();}
      }

      case 'start_verified_workspace_producer': {
        requireStateArg(args,'workspace_handle','workspace_handle');
        let pipeline,gateRel,resultRel,artifactRels;try{pipeline=verifiedPipelineName(args.pipeline);gateRel=verifiedRelativePath(args.gate_path,'gate_path');resultRel=verifiedRelativePath(args.result_json,'result_json');if(!Array.isArray(args.artifacts)||!args.artifacts.length)throw new Error('artifacts must be a non-empty array');artifactRels=args.artifacts.map((x,i)=>verifiedRelativePath(x,`artifacts[${i}]`));}catch(err){return {isError:true,content:[{type:'text',text:`Verified producer input rejected: ${err.message}`}],structuredContent:{state:'verified_input_rejected'}};}
        const pre=workspaceControl.validateWorkspaceHandle(args.workspace_handle,['open']);const releaseProject=await workspaceControl.acquireProjectLock(pre.record.project_path);
        try{
          const validated=workspaceControl.validateWorkspaceHandle(args.workspace_handle,['open']);const preManifest=readJson(workspacePaths(validated.record.workspace_id).manifest);if(!preManifest)throw new WorkspaceControlError('WORKSPACE_PHYSICAL_STATE_MISSING','Producer workspace manifest missing.');if(!workspaceMutableAllows(preManifest,gateRel))return {isError:true,content:[{type:'text',text:`Verified producer gate_path must be inside a declared mutable path: ${gateRel}`}],structuredContent:{state:'gate_not_mutable',workspace_id:preManifest.id,gate_path:gateRel}};ensureWorkspaceIdle(validated.record.workspace_id,'producer workspace');
          const state=await consumeWorkspaceAction(args.workspace_handle,'start_verified_workspace_producer',['open']); const {wp,manifest}=state;
          const gateArgs=['python3',VERIFIED_GATE_BIN,'producer','--project-root','.', '--gate',gateRel,'--pipeline',pipeline,'--result-json',resultRel];for(const rel of artifactRels)gateArgs.push('--artifact',rel);gateArgs.push('--expect-json',JSON.stringify(args.expectations||{}),'--metadata-json',JSON.stringify(args.metadata||{}),'--command',args.command);
          const network=args.network===true,timeout=verifiedTimeoutMs(args.timeout_ms),wrapped=verifiedWorkspaceSandboxCommand(manifest,gateArgs.map(shellQuote).join(' '),network);const job=spawnTrackedProcess(wrapped,wp.work,{id:args.__mcpx_logical_id,register:true,kind:'verified_producer',operationId:args.operation_id,keepStdinOpen:false,timeoutMs:timeout});
          const payload={...commandJobSummary(job),workspace_id:manifest.id,workspace_path:wp.work,workspace_handle:state.next_handle,pipeline,gate_path:gateRel,result_json:resultRel,artifacts:artifactRels,verified_role:'producer',network};return {content:[{type:'text',text:JSON.stringify(payload,null,2)}],structuredContent:payload};
        }finally{await releaseProject();}
      }

      case 'start_verified_workspace_consumer': {
        requireStateArg(args,'workspace_handle','workspace_handle');requireStateArg(args,'candidate_workspace_handle','candidate_workspace_handle');requireStateArg(args,'candidate_build_id','candidate_build_id');
        let pipeline,gateRel,required;try{pipeline=verifiedPipelineName(args.pipeline);gateRel=verifiedRelativePath(args.gate_path,'gate_path');required=Array.isArray(args.required_artifacts)?args.required_artifacts.map((x,i)=>verifiedRelativePath(x,`required_artifacts[${i}]`)):[];}catch(err){return {isError:true,content:[{type:'text',text:`Verified consumer input rejected: ${err.message}`}],structuredContent:{state:'verified_input_rejected'}};}
        const consumerPre=workspaceControl.validateWorkspaceHandle(args.workspace_handle,['open']),candidatePre=workspaceControl.validateWorkspaceHandle(args.candidate_workspace_handle,['open']);if(path.resolve(consumerPre.record.project_path)!==path.resolve(candidatePre.record.project_path))throw new WorkspaceControlError('CROSS_PROJECT_CONSUMER_REFUSED','Verified consumer and candidate workspaces must belong to the same canonical project.');const releaseProject=await workspaceControl.acquireProjectLock(consumerPre.record.project_path);
        try{
          const consumerNow=workspaceControl.validateWorkspaceHandle(args.workspace_handle,['open']),candidateNow=workspaceControl.validateWorkspaceHandle(args.candidate_workspace_handle,['open']);ensureWorkspaceIdle(consumerNow.record.workspace_id,'consumer workspace');ensureWorkspaceIdle(candidateNow.record.workspace_id,'candidate workspace');
          const gate=readReadyBuild(candidateNow.record.workspace_id,gateRel);if(String(gate.build_id)!==String(args.candidate_build_id))throw new WorkspaceControlError('READY_BUILD_MISMATCH',`Requested candidate_build_id=${args.candidate_build_id} but READY gate contains ${gate.build_id}.`,{candidate_workspace_id:candidateNow.record.workspace_id,gate_path:gateRel,current_build_id:gate.build_id});if(gate.pipeline!==pipeline)throw new WorkspaceControlError('READY_PIPELINE_MISMATCH',`READY gate pipeline=${gate.pipeline} does not match ${pipeline}.`);
          const candidateState=await consumeWorkspaceAction(args.candidate_workspace_handle,'start_verified_workspace_consumer_candidate_pin',['open']);const consumerState=await consumeWorkspaceAction(args.workspace_handle,'start_verified_workspace_consumer',['open']);
          const cwp=candidateState.wp,wp=consumerState.wp,manifest=consumerState.manifest,candidate=candidateState.manifest;const gateArgs=['python3',VERIFIED_GATE_BIN,'consumer','--candidate-root',cwp.work,'--gate',gateRel,'--pipeline',pipeline,'--cwd',wp.work];for(const rel of required)gateArgs.push('--artifact',rel);gateArgs.push('--command',args.command);
          const network=args.network===true,timeout=verifiedTimeoutMs(args.timeout_ms),wrapped=verifiedWorkspaceSandboxCommand(manifest,gateArgs.map(shellQuote).join(' '),network);const job=spawnTrackedProcess(wrapped,wp.work,{id:args.__mcpx_logical_id,register:true,kind:'verified_consumer',operationId:args.operation_id,keepStdinOpen:false,timeoutMs:timeout});
          const payload={...commandJobSummary(job),workspace_id:manifest.id,workspace_path:wp.work,workspace_handle:consumerState.next_handle,candidate_workspace_id:candidate.id,candidate_workspace_path:cwp.work,candidate_workspace_handle:candidateState.next_handle,candidate_build_id:gate.build_id,pipeline,gate_path:gateRel,required_artifacts:required,verified_role:'consumer',network};return {content:[{type:'text',text:JSON.stringify(payload,null,2)}],structuredContent:payload};
        }finally{await releaseProject();}
      }

      case 'integrate_agent_workspace': {
        requireStateArg(args,'workspace_handle','workspace_handle');
        const pre=workspaceControl.validateWorkspaceHandle(args.workspace_handle,['open']); const projectRelease=await workspaceControl.acquireIntegrationLock();
        try{
          workspaceControl.validateWorkspaceHandle(args.workspace_handle,['open']);ensureWorkspaceIdle(pre.record.workspace_id);
          if(args.dry_run===true){const wp=workspacePaths(pre.record.workspace_id),manifest=readJson(wp.manifest);await fs.promises.rm(wp.conflicts,{recursive:true,force:true});const planned=await planWorkspaceIntegration(manifest);const summary={workspace_id:manifest.id,workspace_handle:args.workspace_handle,project_path:manifest.project_path,planned_actions:planned.plan.map(x=>({rel:x.rel,action:x.action,reason:x.reason||null,conflict_file:x.conflict_file||null})),conflicts:planned.conflicts,dry_run:true};if(planned.conflicts.length)return {isError:true,content:[{type:'text',text:`Integration blocked by ${planned.conflicts.length} conflict(s); canonical project was not modified.`}],structuredContent:{...summary,status:'conflict'}};return {content:[{type:'text',text:'Integration dry run is clean; no canonical files were modified.'}],structuredContent:{...summary,status:'clean_dry_run'}};}
          const state=await consumeWorkspaceAction(args.workspace_handle,'integrate_agent_workspace',['open']); const {wp,manifest}=state;ensureWorkspaceIdle(manifest.id);
          await fs.promises.rm(wp.conflicts,{recursive:true,force:true});const planned=await planWorkspaceIntegration(manifest);const summary={workspace_id:manifest.id,workspace_handle:state.next_handle,project_path:manifest.project_path,planned_actions:planned.plan.map(x=>({rel:x.rel,action:x.action,reason:x.reason||null,conflict_file:x.conflict_file||null})),conflicts:planned.conflicts,dry_run:false};
          if(planned.conflicts.length){await fs.promises.mkdir(wp.integrations,{recursive:true,mode:0o700});const rec={...summary,status:'conflict',at:new Date().toISOString()};durableWriteJson(path.join(wp.integrations,`${Date.now()}-conflict.json`),rec);return {isError:true,content:[{type:'text',text:`Integration blocked by ${planned.conflicts.length} conflict(s); canonical project was not modified.`}],structuredContent:rec};}
          const applied=await applyWorkspacePlan(manifest,planned);await fs.promises.mkdir(wp.integrations,{recursive:true,mode:0o700});const rec={...summary,...applied,at:new Date().toISOString()};durableWriteJson(path.join(wp.integrations,`${Date.now()}-${applied.status}.json`),rec);if(applied.status==='integrated'){manifest.status='integrated';manifest.integrated_at=new Date().toISOString();durableWriteJson(wp.manifest,manifest);const marked=workspaceControl.markIntegrated(manifest.id,{integrated_at:manifest.integrated_at,applied_count:applied.applied.length});return {content:[{type:'text',text:`Integrated workspace ${manifest.id}: ${applied.applied.length} canonical action(s).`}],structuredContent:{...rec,workspace_handle:null,project_state_token:marked.project_state_token}};}return {isError:true,content:[{type:'text',text:`Integration stopped after a CAS/apply failure; inspect integration record before retry.`}],structuredContent:rec};
        }finally{await projectRelease();}
      }

      case 'remove_agent_workspace': {
        requireStateArg(args,'workspace_handle','workspace_handle');
        const pre=workspaceControl.validateWorkspaceHandle(args.workspace_handle,['open']); const projectRelease=await workspaceControl.acquireProjectLock(pre.record.project_path);
        try{
          workspaceControl.validateWorkspaceHandle(args.workspace_handle,['open']);ensureWorkspaceIdle(pre.record.workspace_id); const wp=workspacePaths(pre.record.workspace_id),manifest=readJson(wp.manifest); if(!manifest)throw new WorkspaceControlError('WORKSPACE_PHYSICAL_STATE_MISSING','Workspace manifest missing.'); const changed=await workspaceChanged(manifest);
          if(changed.length&&!args.force)return {isError:true,content:[{type:'text',text:`Workspace has ${changed.length} unintegrated changed path(s); integrate it or explicitly force retirement (which preserves the full workspace outside the open namespace).`}],structuredContent:{workspace_id:manifest.id,state:'dirty_retirement_refused',changed_paths:changed,workspace_handle:args.workspace_handle}};
          const state=await consumeWorkspaceAction(args.workspace_handle,'remove_agent_workspace',['open']);ensureWorkspaceIdle(manifest.id); const storageRoot=path.join(RECOVERY_DIR,changed.length?'workspace-quarantine':'workspace-retired','physical');await fs.promises.mkdir(storageRoot,{recursive:true,mode:0o700});const dst=path.join(storageRoot,manifest.id);if(fs.existsSync(dst))throw new WorkspaceControlError('RETIREMENT_STORAGE_EXISTS',`Retirement storage already exists for ${manifest.id}.`);
          const marked=workspaceControl.markRetired(manifest.id,args.reason||'explicit_retirement',{physical_present:true,pending_retirement_path:dst,changed_paths:changed,preserved_dirty:changed.length>0});await fs.promises.rename(state.wp.dir,dst);workspaceControl.finalizePhysicalState(manifest.id,{physical_present:false,physical_workspace_path:null,retired_storage_path:dst,pending_retirement_path:null});const payload={workspace_id:manifest.id,status:'retired',changed_paths:changed,preserved_at:dst,workspace_handle:null,project_state_token:marked.project_state_token};return {content:[{type:'text',text:`Retired workspace ${manifest.id}; ${changed.length?'dirty state preserved':'clean state retained'} outside the open workspace namespace.`}],structuredContent:payload};
        }finally{await projectRelease();}
      }

      case 'resume_workspace': {
        requireStateArg(args,'workspace_handle','workspace_handle');requireStateArg(args,'project_state_token','project_state_token');
        const pre=workspaceControl.validateWorkspaceHandle(args.workspace_handle,['suspended']); const project=workspaceControl.validateProjectToken(args.project_state_token,pre.record.project_path); const release=await workspaceControl.acquireTopologyLock();
        try{workspaceControl.validateWorkspaceHandle(args.workspace_handle,['suspended']);workspaceControl.validateProjectToken(args.project_state_token,project.project_path);const rec=pre.record;const src=rec.suspension?.storage_path||rec.suspended_storage_path;const dst=workspacePaths(rec.workspace_id).dir;const overlaps=workspaceControl.openRecords().filter(r=>r.workspace_id!==rec.workspace_id&&r.state==='open'&&workspaceControl.canonicalPathsOverlap(rec.project_path,rec.mutable_paths,r.project_path,r.mutable_paths));if(overlaps.length&&!['independent_audit','independent_branch'].includes(rec.role))throw new WorkspaceControlError('RESUME_OVERLAP',`Cannot resume development workspace while ${overlaps.length} overlapping open workspace(s) exist.`,{overlapping:overlaps.map(r=>r.workspace_id)});workspaceControl.finalizePhysicalState(rec.workspace_id,{resume_pending:true});let recoveredMove=false;if(fs.existsSync(dst)&&(!src||!fs.existsSync(src))){recoveredMove=true;}else{if(!src||!fs.existsSync(src))throw new WorkspaceControlError('SUSPENSION_STORAGE_MISSING',`Suspended payload is missing for ${rec.workspace_id}.`);if(fs.existsSync(dst))throw new WorkspaceControlError('RESUME_DESTINATION_EXISTS',`Open workspace path already exists for ${rec.workspace_id}.`);await fs.promises.rename(src,dst);}const marked=workspaceControl.markResumed(rec.workspace_id);workspaceControl.finalizePhysicalState(rec.workspace_id,{physical_present:true,physical_workspace_path:dst,suspension:null,resume_pending:null});const payload={workspace_id:rec.workspace_id,workspace_path:workspacePaths(rec.workspace_id).work,status:'open',workspace_handle:marked.workspace_handle,project_state_token:marked.project_state_token,recovered_move:recoveredMove};return {content:[{type:'text',text:`Resumed suspended workspace ${rec.workspace_id}.`}],structuredContent:payload};}finally{await release();}
      }

      case 'start_command': {
        const execCwd = resolvePath(args.cwd); rejectRawWorkspacePath(execCwd,'cwd');
        const startTimeoutMs = args.timeout_ms === 0 ? null : Math.min(Math.max(Number(args.timeout_ms) || 24 * 60 * 60 * 1000, 60 * 1000), 7 * 24 * 60 * 60 * 1000);
        const job = spawnTrackedProcess(args.command, execCwd, { id: args.__mcpx_logical_id, register: true, kind: 'background', operationId: args.operation_id, keepStdinOpen: args.keep_stdin_open === true, timeoutMs: startTimeoutMs });
        return { content: [{ type: 'text', text: JSON.stringify(commandJobSummary(job), null, 2) }] };
      }

      case 'command_status': {
        const job = commandJobs.get(args.job_id);
        if (!job) return { isError: true, content: [{ type: 'text', text: `Unknown command job: ${args.job_id}` }] };
        return { content: [{ type: 'text', text: JSON.stringify(commandJobSnapshot(job, args), null, 2) }] };
      }

      case 'send_command_input': {
        const job = commandJobs.get(args.job_id);
        if (!job) return { isError: true, content: [{ type: 'text', text: `Unknown command job: ${args.job_id}` }] };
        refreshJob(job);
        if (job.status !== 'running') {
          return { isError: true, content: [{ type: 'text', text: `Job ${args.job_id} is not running (status=${job.status})` }] };
        }
        if (!job.child?.stdin || job.child.stdin.destroyed) {
          return { isError: true, content: [{ type: 'text', text: `Job ${args.job_id} survived a server restart, but its stdin pipe is no longer attachable. Status/output/stop remain available.` }] };
        }
        job.child.stdin.write(args.input);
        if (args.close_stdin) job.child.stdin.end();
        return { content: [{ type: 'text', text: `Sent ${Buffer.byteLength(args.input, 'utf8')} bytes to ${args.job_id}${args.close_stdin ? ' and closed stdin' : ''}` }] };
      }

      case 'stop_command': {
        const job = commandJobs.get(args.job_id);
        if (!job) return { isError: true, content: [{ type: 'text', text: `Unknown command job: ${args.job_id}` }] };
        refreshJob(job);
        if (job.status !== 'running') return { content: [{ type: 'text', text: JSON.stringify(commandJobSummary(job), null, 2) }] };
        const signal = args.force ? 'SIGKILL' : 'SIGTERM';
        job.terminationRequested = signal;
        persistJob(job);
        killJobGroup(job, signal);
        return { content: [{ type: 'text', text: `Sent ${signal} to command job ${args.job_id}` }] };
      }

      case 'list_command_jobs': {
        const allJobs = Array.from(commandJobs.values()).map(commandJobSummary).sort((a,b)=>b.started_at.localeCompare(a.started_at) || a.job_id.localeCompare(b.job_id));
        const totalJobs=allJobs.length;
        const cursor=typeof args.cursor==='string' ? args.cursor : null;
        let eligible=allJobs;
        if(cursor){const cut=cursor.lastIndexOf('|'); if(cut>0){const ts=cursor.slice(0,cut),id=cursor.slice(cut+1);eligible=allJobs.filter(j=>j.started_at < ts || (j.started_at===ts && j.job_id>id));}}
        const limit=Math.min(200,Math.max(1,Math.floor(Number(args.limit)||50)));
        const jobs=eligible.slice(0,limit);
        const remaining=eligible.length>jobs.length;
        const last=jobs[jobs.length-1];
        const nextCursor=remaining&&last?`${last.started_at}|${last.job_id}`:null;
        const payload={total_jobs:totalJobs,cursor,limit,returned:jobs.length,truncated:remaining,next_cursor:nextCursor,items:jobs};
        return { content: [{ type:'text', text:JSON.stringify(payload,null,2) }] };
      }

      case 'execute_command': {
        let execCwd,spawnCommand=args.command,workspaceMeta={workspace_id:null,workspace_handle:null};
        if(args.workspace_handle){
          const rel=args.cwd || '.';const resolved=workspaceControl.resolveWorkspaceRelative(args.workspace_handle,rel,['open'],false);ensureWorkspaceIdle(resolved.record.workspace_id,'execute workspace');const state=await consumeWorkspaceAction(args.workspace_handle,'execute_command',['open']);
          const inner=rel==='.'?args.command:`cd ${shellQuote(rel)} && ${args.command}`;spawnCommand=verifiedWorkspaceSandboxCommand(state.manifest,inner,false);execCwd=state.wp.work;workspaceMeta={workspace_id:state.record.workspace_id,workspace_handle:state.next_handle,workspace_cwd:rel};
        }else{execCwd=resolvePath(args.cwd);rejectRawWorkspacePath(execCwd,'cwd');}
        const requestedTimeout = Math.min(Math.max(Number(args.timeout_ms) || 120000, 1000), 24 * 60 * 60 * 1000);
        const promoteAfterMs = 20000;
        const job = spawnTrackedProcess(spawnCommand, execCwd, { id: args.__mcpx_logical_id, register: false, kind: 'execute', operationId: args.operation_id, timeoutMs: requestedTimeout });

        return new Promise((resolve) => {
          let responseSettled = false;
          let promoted = false;
          let settlePollTimer = null;

          const cleanupEphemeral = () => {
            if (!promoted) {
              commandJobs.delete(job.id);
              job.persisted = false;
              if (job.timeoutTimer) { clearTimeout(job.timeoutTimer); job.timeoutTimer = null; }
              if (job.logGuardTimer) { clearInterval(job.logGuardTimer); job.logGuardTimer = null; }
              if (job.lifecycleTimer) { clearInterval(job.lifecycleTimer); job.lifecycleTimer = null; }
              setTimeout(() => removeJobFiles(job), 50).unref();
            }
          };

          const promoteTimer = setTimeout(async () => {
            refreshJob(job);
            if (responseSettled || job.status !== 'running') return;
            promoted = true;
            registerJob(job);
            responseSettled = true;
            if (settlePollTimer) { clearTimeout(settlePollTimer); settlePollTimer = null; }
            if (requestSignal) requestSignal.removeEventListener('abort', abortHandler);
            resolve({
              content: [{
                type: 'text',
                text: JSON.stringify({
                  promoted_to_background: true,
                  message: `Command exceeded ${promoteAfterMs}ms synchronous window and is continuing as a persistent background job.`,
                  ...commandJobSummary(job),
                  ...workspaceMeta,
                }, null, 2),
              }],
            });
          }, promoteAfterMs);
          promoteTimer.unref();

          const requestSignal = ctx?.mcpReq?.signal;
          const abortHandler = () => {
            refreshJob(job);
            if (!promoted && job.status === 'running') {
              job.terminationRequested = 'SIGTERM';
              killJobGroup(job, 'SIGTERM');
            }
          };
          if (requestSignal) {
            if (requestSignal.aborted) abortHandler();
            else requestSignal.addEventListener('abort', abortHandler, { once: true });
          }

          const finishResponse = async () => {
            refreshJob(job);
            if (responseSettled) return;
            if (job.status === 'running') {
              if (!settlePollTimer) {
                settlePollTimer = setTimeout(() => { settlePollTimer = null; finishResponse(); }, 50);
                settlePollTimer.unref();
              }
              return;
            }
            clearTimeout(promoteTimer);
            if (settlePollTimer) { clearTimeout(settlePollTimer); settlePollTimer = null; }
            if (requestSignal) requestSignal.removeEventListener('abort', abortHandler);
            responseSettled = true;
            const out = readOutputPrefix(job.paths.stdout, EXEC_RESPONSE_MAX_BYTES);
            const err = readOutputPrefix(job.paths.stderr, EXEC_RESPONSE_MAX_BYTES);
            const exitCode = job.timedOut ? 124 : (job.exitCode ?? (job.status === 'exited' ? 0 : 1));
            resolve({
              isError: job.timedOut || exitCode !== 0,
              content: [{ type: 'text', text: JSON.stringify({
                command: args.command,
                cwd: execCwd,
                exitCode,
                signal: job.signal,
                timedOut: !!job.timedOut,
                stdout: out.text,
                stderr: err.text,
                stdout_total_bytes: out.size,
                stderr_total_bytes: err.size,
                stdout_truncated: out.size > out.next,
                stderr_truncated: err.size > err.next,
                output_limit_bytes_per_stream: EXEC_RESPONSE_MAX_BYTES,
                ...workspaceMeta,
              }, null, 2) }],
            });
            cleanupEphemeral();
          };

          job.child.once('error', finishResponse);
          job.child.once('exit', finishResponse);
        });
      }

      default:
        return { isError: true, content: [{ type: 'text', text: `Unknown tool: ${name}` }] };
    }
  } catch (err) {
    if(err instanceof WorkspaceControlError) return workspaceControlErrorResult(err);
    return { isError: true, content: [{ type: 'text', text: `Tool error: ${err.message}` }], structuredContent:{state:'tool_error',text:String(err.message||err)} };
  }
  };
  const result = ensureStructuredContentCompatibility(await runTool(), name);
  try { return await finishSideEffectOperation(operation, result); }
  catch (err) { await abandonOperationLock(operation); return operationResultError(`Operation result persistence failed after tool execution; automatic replay is disabled: ${err.message}`, { operation_id: operation?.operation_id || null, tool:name, state:'uncertain' }); }
});


  return server;
}

async function readRequestBody(req, maxBytes) {
  if (req.method === 'GET' || req.method === 'HEAD' || req.method === 'DELETE') return Buffer.alloc(0);
  const declared = Number(req.headers['content-length']);
  if (Number.isFinite(declared) && declared > maxBytes) {
    const err = new Error(`Request body exceeds ${maxBytes} bytes`);
    err.statusCode = 413;
    throw err;
  }
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > maxBytes) {
      const err = new Error(`Request body exceeds ${maxBytes} bytes`);
      err.statusCode = 413;
      throw err;
    }
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

function nodeHeadersToWeb(headers) {
  const out = new Headers();
  for (const [key, value] of Object.entries(headers)) {
    if (value === undefined) continue;
    if (Array.isArray(value)) for (const item of value) out.append(key, item);
    else out.set(key, String(value));
  }
  return out;
}

function validateLocalHost(req) {
  const raw = req.headers.host;
  if (!raw) return false;
  try {
    const hostname = new URL(`http://${raw}`).hostname;
    return hostname === '127.0.0.1' || hostname === 'localhost' || hostname === '[::1]' || hostname === '::1';
  } catch (_) {
    return false;
  }
}

function makeWebRequest(req, body, abortController) {
  const host = req.headers.host || '127.0.0.1';
  const init = {
    method: req.method,
    headers: nodeHeadersToWeb(req.headers),
    signal: abortController.signal,
  };
  if (body.length && req.method !== 'GET' && req.method !== 'HEAD') init.body = body;
  return new Request(`http://${host}${req.url || '/'}`, init);
}

async function writeWebResponse(webResponse, res, abortController, receiptContext = null) {
  const headers = {};
  for (const [key, value] of webResponse.headers.entries()) headers[key] = value;
  const contentType = String(webResponse.headers.get('content-type') || '').toLowerCase();

  // Finite JSON responses are buffered once so the exact body size/hash at the MCP HTTP
  // boundary can be recorded. SSE/streaming responses retain the original streaming path.
  if (contentType.includes('application/json')) {
    let body = Buffer.from(await webResponse.arrayBuffer());
    const originalBody = body;
    const originalSha = crypto.createHash('sha256').update(originalBody).digest('hex');
    const predictedTransportBytes = originalBody.length + TRANSPORT_ENVELOPE_BYTES;
    let spill = null;
    let spillReason = null;
    const semanticSoftBudgetApplies = receiptContext && ['execute_command','command_status'].includes(receiptContext.tool) && originalBody.length > SEMANTIC_RESPONSE_LIMIT_BYTES;
    if (semanticSoftBudgetApplies) {
      spill = spillJsonRpcResult(originalBody, receiptContext);
      spillReason = 'semantic_model_facing_response_budget';
      body = compactPreservedResponse(originalBody, spill, spillReason, SEMANTIC_RESPONSE_LIMIT_BYTES, receiptContext);
    } else if (receiptContext && originalBody.length > MODEL_RESPONSE_LIMIT_BYTES) {
      spill = spillJsonRpcResult(originalBody, receiptContext);
      spillReason = 'model_facing_response_budget';
      body = compactPreservedResponse(originalBody, spill, spillReason, MODEL_RESPONSE_LIMIT_BYTES, receiptContext);
    } else if (receiptContext && TRANSPORT_LIMIT_ENABLED && predictedTransportBytes > TRANSPORT_POST_LIMIT_BYTES) {
      spill = spillJsonRpcResult(originalBody, receiptContext);
      spillReason = 'configured_transport_response_limit';
      body = compactPreservedResponse(originalBody, spill, spillReason, TRANSPORT_POST_LIMIT_BYTES, receiptContext);
    }
    const deliveredSha = crypto.createHash('sha256').update(body).digest('hex');
    if (receiptContext) {
      appendReceipt({
        timestamp: new Date().toISOString(),
        duration_ms: Date.now() - receiptContext.startedAt,
        protocol_version: receiptContext.protocolVersion,
        tool: receiptContext.tool,
        rpc_id: receiptContext.rpcId,
        request_bytes: receiptContext.requestBytes,
        args: receiptContext.args,
        local_http_status: webResponse.status,
        transport_limit_enabled: TRANSPORT_LIMIT_ENABLED,
        transport_post_limit_bytes: TRANSPORT_POST_LIMIT_BYTES || null,
        transport_envelope_bytes: TRANSPORT_ENVELOPE_BYTES,
        original_local_mcp_response_bytes: originalBody.length,
        predicted_transport_bytes: predictedTransportBytes,
        overflow_spilled: !!spill,
        overflow_reason: spillReason,
        semantic_response_limit_bytes: SEMANTIC_RESPONSE_LIMIT_BYTES,
        model_response_limit_bytes: MODEL_RESPONSE_LIMIT_BYTES,
        spill_result_id: spill?.result_id || null,
        delivered_local_mcp_response_bytes: body.length,
        original_response_sha256: originalSha,
        delivered_response_sha256: deliveredSha,
      });
    }
    res.writeHead(webResponse.status, webResponse.statusText || undefined, { ...headers, 'content-length': String(body.length) });
    res.end(body);
    return;
  }

  res.writeHead(webResponse.status, webResponse.statusText || undefined, headers);
  // Expose SSE headers immediately rather than waiting for the first keepalive/data frame.
  res.flushHeaders?.();
  if (!webResponse.body) {
    res.end();
    return;
  }
  const readable = Readable.fromWeb(webResponse.body);
  const onClose = () => {
    if (!res.writableEnded) {
      abortController.abort();
      readable.destroy();
    }
  };
  res.once('close', onClose);
  readable.pipe(res);
  try {
    await finished(readable);
  } catch (err) {
    if (!abortController.signal.aborted) throw err;
  } finally {
    res.off('close', onClose);
  }
}

async function main() {
  await recoverFileTransactions();
  const host = process.env.MCP_HTTP_HOST || '127.0.0.1';
  const port = Number(process.env.MCP_HTTP_PORT || 9090);
  const endpoint = process.env.MCP_HTTP_ENDPOINT || '/mcp';
  const maxBodyBytes = Number(process.env.MCP_HTTP_MAX_BODY_BYTES || 20 * 1024 * 1024);
  const legacySessions = new Map();
  const legacySessionIdleMs = Math.max(1000, Number(process.env.MCP_LEGACY_SESSION_IDLE_MS) || 30 * 60 * 1000);
  const legacySessionSweepMs = Math.min(60 * 1000, Math.max(1000, Math.floor(legacySessionIdleMs / 2)));

  // 2026-07-28+ leg: sessionless, fresh protocol instance per request.
  const modernHandler = createMcpHandler(() => createMcpServer(), {
    legacy: 'reject',
    onerror: (err) => console.error('[modern MCP]', err),
  });

  async function newLegacySession(webRequest, parsedBody) {
    const requestServer = createMcpServer();
    let sessionId;
    let cleaning = false;
    const transport = new WebStandardStreamableHTTPServerTransport({
      sessionIdGenerator: () => crypto.randomUUID(),
      keepAliveMs: 15000,
      onsessioninitialized: (sid) => {
        sessionId = sid;
        legacySessions.set(sid, { server: requestServer, transport, lastActivityAt: Date.now(), openStreams: 0, closing: false });
      },
      onsessionclosed: (sid) => legacySessions.delete(sid),
    });
    transport.onclose = async () => {
      if (cleaning) return;
      cleaning = true;
      if (sessionId) legacySessions.delete(sessionId);
      try { await requestServer.close(); } catch (_) {}
    };
    transport.onerror = (err) => console.error('[legacy transport]', err);
    await requestServer.connect(transport);
    try {
      return await transport.handleRequest(webRequest, { parsedBody });
    } catch (err) {
      try { await requestServer.close(); } catch (_) {}
      throw err;
    }
  }

  async function handleLegacy(webRequest, parsedBody) {
    const sid = webRequest.headers.get('mcp-session-id');
    if (sid) {
      const entry = legacySessions.get(sid);
      if (!entry) {
        return new Response(JSON.stringify({
          jsonrpc: '2.0',
          error: { code: -32001, message: 'Session not found' },
          id: parsedBody && typeof parsedBody === 'object' ? (parsedBody.id ?? null) : null,
        }), { status: 404, headers: { 'content-type': 'application/json' } });
      }
      entry.lastActivityAt = Date.now();
      return entry.transport.handleRequest(webRequest, parsedBody === undefined ? undefined : { parsedBody });
    }
    if (webRequest.method === 'POST' && isInitializeRequest(parsedBody)) {
      return newLegacySession(webRequest, parsedBody);
    }
    return new Response(JSON.stringify({
      jsonrpc: '2.0',
      error: { code: -32000, message: 'Bad Request: No valid session ID provided' },
      id: parsedBody && typeof parsedBody === 'object' ? (parsedBody.id ?? null) : null,
    }), { status: 400, headers: { 'content-type': 'application/json' } });
  }

  const legacySessionReaper = setInterval(() => {
    const now = Date.now();
    for (const [sid, entry] of legacySessions) {
      if (entry.closing || entry.openStreams > 0 || now - entry.lastActivityAt <= legacySessionIdleMs) continue;
      entry.closing = true;
      entry.server.close().catch((err) => console.error(`[legacy session reaper] failed to close ${sid}:`, err));
    }
  }, legacySessionSweepMs);
  legacySessionReaper.unref();

  const httpServer = http.createServer(async (req, res) => {
    const abortController = new AbortController();
    const abortIfClientGone = () => {
      if (!res.writableEnded && !abortController.signal.aborted) abortController.abort();
    };
    req.once('aborted', abortIfClientGone);
    res.once('close', abortIfClientGone);
    try {
      if (!validateLocalHost(req)) {
        res.writeHead(403, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ jsonrpc: '2.0', error: { code: -32000, message: 'Invalid Host header' }, id: null }));
        return;
      }
      const pathname = new URL(req.url || '/', `http://${req.headers.host}`).pathname;
      if (pathname === '/healthz' || pathname === '/readyz') {
        res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
        res.end(pathname === '/healthz' ? 'live' : 'ready');
        return;
      }
      if (pathname !== endpoint) {
        res.writeHead(404).end();
        return;
      }

      const bodyBuffer = await readRequestBody(req, maxBodyBytes);
      let parsedBody;
      if (bodyBuffer.length) {
        try { parsedBody = JSON.parse(bodyBuffer.toString('utf8')); }
        catch (err) {
          res.writeHead(400, { 'content-type': 'application/json' });
          res.end(JSON.stringify({ jsonrpc: '2.0', error: { code: -32700, message: `Parse error: ${err.message}` }, id: null }));
          return;
        }
      }
      const webRequest = makeWebRequest(req, bodyBuffer, abortController);
      const receiptContext = buildReceiptContext(req, parsedBody, bodyBuffer.length);
      const legacy = await isLegacyRequest(webRequest, parsedBody);
      let legacyStreamEntry = null;
      if (legacy && req.method === 'GET') {
        const sid = webRequest.headers.get('mcp-session-id');
        const entry = sid ? legacySessions.get(sid) : null;
        if (entry) {
          entry.openStreams += 1;
          entry.lastActivityAt = Date.now();
          legacyStreamEntry = entry;
        }
      }
      try {
        const webResponse = legacy
          ? await handleLegacy(webRequest, parsedBody)
          : await modernHandler.fetch(webRequest, parsedBody === undefined ? undefined : { parsedBody });
        await writeWebResponse(webResponse, res, abortController, receiptContext);
      } finally {
        if (legacyStreamEntry) {
          legacyStreamEntry.openStreams = Math.max(0, legacyStreamEntry.openStreams - 1);
          legacyStreamEntry.lastActivityAt = Date.now();
        }
      }
    } catch (err) {
      console.error('[HTTP MCP]', err);
      if (!res.headersSent) {
        const status = err.statusCode || 500;
        res.writeHead(status, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ jsonrpc: '2.0', error: { code: -32603, message: err.message || 'Internal server error' }, id: null }));
      } else if (!res.writableEnded) {
        res.destroy(err);
      }
    }
  });
  httpServer.keepAliveTimeout = 300000;
  httpServer.headersTimeout = 301000;

  await new Promise((resolve, reject) => {
    httpServer.once('error', reject);
    httpServer.listen(port, host, () => {
      httpServer.off('error', reject);
      resolve();
    });
  });
  console.error(`Native dual-era MCP HTTP listening at http://${host}:${port}${endpoint} version=${MCPX_SERVER_VERSION} build=${MCPX_BUILD_ID} instance=${MCPX_SERVER_INSTANCE_ID}`);

  const shutdown = async () => {
    clearInterval(legacySessionReaper);
    for (const entry of legacySessions.values()) {
      try { await entry.server.close(); } catch (_) {}
    }
    legacySessions.clear();
    try { await modernHandler.close(); } catch (_) {}
    await new Promise((resolve) => httpServer.close(() => resolve()));
  };
  process.once('SIGTERM', () => shutdown().finally(() => process.exit(0)));
  process.once('SIGINT', () => shutdown().finally(() => process.exit(0)));
}

main().catch((err) => {
  console.error('Fatal server error:', err);
  process.exit(1);
});
