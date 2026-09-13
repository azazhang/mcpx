#!/usr/bin/env python3
from pathlib import Path
import json, os, shutil, signal, subprocess, sys, threading, time
sys.path.insert(0,str(Path(__file__).parent))
from mcp_test_client import MCPClient, sc

ROOT=Path(__file__).resolve().parent.parent
T=Path(os.environ.get('MCPX_CLAIM_TEST_RUNTIME',str(ROOT/'workspace-control/claim2-runtime'))).resolve(); PORT=int(os.environ.get('MCPX_CLAIM_TEST_PORT','19094')); URL=f'http://127.0.0.1:{PORT}/mcp'
if T.exists(): shutil.rmtree(T)
for d in ['project/mutable','workspaces','recovery','operations','jobs','results','receipts','filetxns']:(T/d).mkdir(parents=True,exist_ok=True)
(T/'project/mutable/file.txt').write_text('base\n')
base_env=os.environ.copy();base_env.update({'BASE_DIR':str(T/'project'),'MCPX_WORKSPACE_ROOT':str(T/'workspaces'),'MCPX_RECOVERY_DIR':str(T/'recovery'),'MCPX_OPERATIONS_DIR':str(T/'operations'),'MCPX_JOB_DIR':str(T/'jobs'),'MCPX_RESULT_DIR':str(T/'results'),'MCPX_RECEIPT_DIR':str(T/'receipts'),'MCPX_FILETXN_DIR':str(T/'filetxns'),'MCPX_FILETXN_BIN':str(ROOT/'bin/mcpx-filetxn-atomic'),'MCPX_VERIFIED_GATE_BIN':str(ROOT/'bin/mcpx-verified-gate'),'MCP_HTTP_HOST':'127.0.0.1','MCP_HTTP_PORT':str(PORT),'MCP_HTTP_ENDPOINT':'/mcp','MCPX_BUILD_ID':'claim-recovery-harness','MCPX_ENABLE_COMMAND_EXECUTION':'1','MCPX_WORKSPACE_SANDBOX':'none'})

def start(env_extra=None):
    env=base_env.copy();env.update(env_extra or {})
    p=subprocess.Popen(['node','server.js'],cwd=ROOT,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,start_new_session=True)
    end=time.time()+10
    while time.time()<end:
        if p.poll() is not None: raise RuntimeError('server exited: '+(p.stderr.read() or ''))
        try:
            c=MCPClient(URL);c.init();return p,c
        except Exception: time.sleep(.05)
    os.killpg(p.pid,signal.SIGKILL);raise RuntimeError('server did not become ready')

def stop(p,sig=signal.SIGTERM):
    if p.poll() is None:
        os.killpg(p.pid,sig)
        try:p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=5)

def find_op(oid):
    for p in (T/'operations').glob('*/meta.json'):
        try:j=json.loads(p.read_text())
        except:continue
        if j.get('operation_id')==oid:return p,j
    return None,None

# Obtain a valid pre-crash project token.
p,c=start();proj=str((T/'project').resolve());r=c.call('reconcile_project_state',{'project_path':proj});token=sc(r)['projects'][0]['project_state_token'];stop(p)
args={'project_path':proj,'project_state_token':token,'mutable_paths':['mutable'],'name':'claim-recovery','operation_id':'pub-claim-harness-create-001'}
marker=T/'claim.marker';errbox={};resbox={}
p,c=start({'MCPX_TEST_OPERATION_CLAIM_MARKER':str(marker),'MCPX_TEST_OPERATION_CLAIM_DELAY_MS':'60000'})
def invoke():
    try:resbox['r']=c.call('create_agent_workspace',args)
    except Exception as e:errbox['e']=repr(e)
th=threading.Thread(target=invoke);th.start()
end=time.time()+10
while time.time()<end and not marker.exists(): time.sleep(.01)
assert marker.exists(),'claim marker missing'
claim=json.loads(marker.read_text());logical=claim['logical_id'];assert claim['state']=='claimed'
# Kill exactly at claimed-before-dispatch boundary.
os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=5);th.join(timeout=10);assert not th.is_alive(),'client did not unblock after server death'
meta_path,meta=find_op(args['operation_id']);assert meta and meta['state']=='claimed',meta
assert not (T/'workspaces'/logical).exists(),f'workspace unexpectedly created before kill: {logical}'
assert not (meta_path.parent/'result.json').exists(),'durable result unexpectedly exists before recovery'
# Restart without delay and replay exact logical operation.
p2,c2=start();r2=c2.call('create_agent_workspace',args);assert not r2.get('isError'),r2;s=sc(r2);assert s['workspace_id']==logical,(s,logical);assert (T/'workspaces'/logical/'manifest.json').exists();assert s.get('workspace_handle')
# Repeating once more must return the same durable result / same workspace, not create a second one.
r3=c2.call('create_agent_workspace',args);assert not r3.get('isError'),r3;assert sc(r3)['workspace_id']==logical
manifests=list((T/'workspaces').glob('*/manifest.json'));assert len(manifests)==1,manifests
_,done=find_op(args['operation_id']);assert done and done['state']=='completed',done
stop(p2)
print(json.dumps({'passed':True,'operation_id':args['operation_id'],'logical_workspace_id':logical,'client_interrupted':bool(errbox),'manifest_count':len(manifests),'final_state':done['state']},indent=2))
