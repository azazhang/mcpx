#!/usr/bin/env python3
from pathlib import Path
import concurrent.futures, hashlib, json, os, subprocess, sys, threading, time, uuid
sys.path.insert(0,str(Path(__file__).parent))
from mcp_test_client import MCPClient, sc, state

ROOT=Path(__file__).resolve().parent.parent
T=Path(os.environ.get('MCPX_TEST_RUNTIME',str(ROOT/'workspace-control/test-runtime'))).resolve()
PROJECT=(T/'project').resolve(); WROOT=T/'workspaces'; REC=T/'recovery'
URL=os.environ.get('MCPX_TEST_URL','http://127.0.0.1:19092/mcp')
REPORT=T/'adversarial-report.json'
checks=[]

def ok(name, detail=None): checks.append({'name':name,'passed':True,'detail':detail}); print('PASS',name, detail or '')
def fail(name, detail): checks.append({'name':name,'passed':False,'detail':detail}); raise AssertionError(f'{name}: {detail}')
def expect(cond,name,detail=''):
    if cond: ok(name,detail)
    else: fail(name,detail)
def client():
    c=MCPClient(URL);c.init();return c

def reconcile(c, detail=False):
    r=c.call('reconcile_project_state',{'project_path':str(PROJECT),'detail':detail})
    expect(not r.get('isError'),'reconcile_project_state',state(r))
    p=sc(r)['projects'][0]
    return p

def nodes_by_name(p): return {x.get('name'):x for x in p.get('open_or_suspended_workspaces',[])}
def call_state(r): return state(r) or (r.get('jsonrpc_error') or {}).get('message')
def age_record(wid, seconds=180):
    p=REC/'workspaces'/f'{wid}.json';j=json.loads(p.read_text());j['updated_at']=time.strftime('%Y-%m-%dT%H:%M:%S.000Z',time.gmtime(time.time()-seconds));tmp=p.with_suffix('.tmp');tmp.write_text(json.dumps(j,indent=2)+'\n');os.replace(tmp,p)
def poll(c,jid, timeout=30):
    end=time.time()+timeout; last=None
    while time.time()<end:
        r=c.call('command_status',{'job_id':jid,'max_chars':4000}); last=sc(r)
        if last.get('status')!='running': return last
        time.sleep(.1)
    fail('job_terminal',f'{jid} last={last}')

def maintenance(dry=False):
    env=os.environ.copy();env.update({
        'MCPX_WORKSPACE_ROOT':str(WROOT),'MCPX_RECOVERY_DIR':str(REC),'MCPX_SERVER_VERSION':'0.1.0-test',
        'MCPX_CLEAN_IDLE_MS':'60000','MCPX_DIVERGENT_COLD_MS':'60000','MCPX_INTEGRATED_GC_GRACE_MS':'60000'
    })
    args=['node','workspace-control/maintenance.js']+(['--dry-run'] if dry else [])
    p=subprocess.run(args,cwd=ROOT,env=env,text=True,capture_output=True,timeout=60)
    if p.returncode not in (0,): fail('maintenance_exit',f'rc={p.returncode} out={p.stdout[-2000:]} err={p.stderr[-2000:]}')
    try:return json.loads(p.stdout)
    except Exception: fail('maintenance_json',p.stdout[-4000:])

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()

c=client();p0=reconcile(c);token0=p0['project_state_token']
# Create development workspace.
create_args={'project_path':str(PROJECT),'project_state_token':token0,'mutable_paths':['mutable'],'name':'dev','operation_id':'pub-create-dev-001'}
r=c.call('create_agent_workspace',create_args);expect(not r.get('isError'),'create_development',call_state(r));dev=sc(r);dev_id=dev['workspace_id'];dev_h=dev['workspace_handle'];token1=dev['project_state_token']
# Exact create retry must recover despite stale project token.
r_retry=c.call('create_agent_workspace',create_args);expect(not r_retry.get('isError') and sc(r_retry).get('workspace_id')==dev_id,'create_exact_retry_after_topology_change',call_state(r_retry))
# Project topology changes do not stale tokens/handles; overlap is checked atomically at the canonical scope.
r=c.call('create_agent_workspace',{'project_path':str(PROJECT),'project_state_token':token0,'mutable_paths':['token-other'],'name':'token-other','operation_id':'pub-create-oldtoken-nonoverlap-001'});expect(not r.get('isError'),'old_project_token_nonoverlap_allowed',call_state(r))
r=c.call('create_agent_workspace',{'project_path':str(PROJECT),'project_state_token':token1,'mutable_paths':['mutable'],'name':'overlap','operation_id':'pub-create-overlap-001'});expect(r.get('isError') and state(r)=='OPEN_WORKSPACE_OVERLAP','implicit_overlap_rejected',call_state(r))
# Explicit independent audit branch allowed.
r=c.call('create_agent_workspace',{'project_path':str(PROJECT),'project_state_token':token1,'mutable_paths':['mutable'],'name':'audit','branch_of_handle':dev_h,'branch_role':'independent_audit','branch_reason':'independent validation','operation_id':'pub-create-audit-001'});expect(not r.get('isError'),'explicit_audit_branch_allowed',call_state(r));audit_id=sc(r)['workspace_id']
# Creating unrelated work packets changed topology, but the dev handle remains valid until dev itself changes.
r=c.call('write_file',{'path':'mutable/topology-handle.txt','content':'still-valid','workspace_handle':dev_h,'operation_id':'pub-topology-handle-still-valid-001'});expect(not r.get('isError'),'unrelated_topology_does_not_stale_workspace_handle',call_state(r));dev_h=sc(r)['workspace_handle']
# Parent/child project roots are one physical canonical namespace for overlap and scoped reconciliation.
NESTED=(PROJECT/'nested').resolve();NESTED.mkdir(parents=True,exist_ok=True);(NESTED/'shared').mkdir(parents=True,exist_ok=True)
p=reconcile(c);tok_parent=p['project_state_token']
r=c.call('create_agent_workspace',{'project_path':str(PROJECT),'project_state_token':tok_parent,'mutable_paths':['nested/shared'],'name':'nested-parent-work','operation_id':'pub-create-nested-parent-001'});expect(not r.get('isError'),'create_parent_root_nested_scope',call_state(r))
rn=c.call('reconcile_project_state',{'project_path':str(NESTED),'detail':False});expect(not rn.get('isError'),'nested_scope_reconcile',call_state(rn));np=sc(rn)['projects'][0];nn=nodes_by_name(np);expect('nested-parent-work' in nn and nn['nested-parent-work'].get('scope_relation')=='ancestor_root','parent_workspace_visible_from_child_scope',nn.get('nested-parent-work'))
r=c.call('create_agent_workspace',{'project_path':str(NESTED),'project_state_token':np['project_state_token'],'mutable_paths':['shared'],'name':'nested-child-overlap','operation_id':'pub-create-nested-child-overlap-001'});expect(r.get('isError') and state(r)=='OPEN_WORKSPACE_OVERLAP','cross_root_physical_overlap_rejected',call_state(r))
# Fresh registry read for subsequent work.
p=reconcile(c,True);ns=nodes_by_name(p);dev_h=ns['dev']['workspace_handle'];audit_h=ns['audit']['workspace_handle'];token=p['project_state_token']
# Raw workspace bypasses fail.
fake=WROOT/'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'/'work'/'mutable'/'fake.txt'
r=c.call('write_file',{'path':str(fake),'content':'x','operation_id':'pub-raw-fake-write-001'});expect(r.get('isError') and state(r)=='RAW_WORKSPACE_PATH_FORBIDDEN','fake_workspace_raw_write_rejected',call_state(r))
r=c.call('execute_command',{'command':'pwd','cwd':str(WROOT/dev_id/'work'),'operation_id':'pub-raw-cwd-001'});expect(r.get('isError') and state(r)=='RAW_WORKSPACE_PATH_FORBIDDEN','raw_workspace_cwd_rejected',call_state(r))
# Stale pre-refresh call cannot mutate. It may fail MCP schema validation before handler.
raw=c.call_raw('start_workspace_command',{'workspace_id':dev_id,'command':'echo unsafe','operation_id':'pub-old-schema-001'})
old_schema_failed=('error' in raw) or raw.get('result',{}).get('isError')
expect(old_schema_failed,'stale_client_schema_fails_closed',str(raw.get('error') or state(raw.get('result',{}))))
# Workspace edit consumes handle; exact retry returns prior result before stale-handle validation.
edit_args={'path':'mutable/file.txt','target':'base','replacement':'changed','workspace_handle':dev_h,'operation_id':'pub-edit-001'}
r=c.call('edit_file',edit_args);expect(not r.get('isError'),'workspace_edit_success',call_state(r));dev_h2=sc(r)['workspace_handle'];edited_sha=sc(r).get('new_sha256')
r2=c.call('edit_file',edit_args);expect(not r2.get('isError') and sc(r2).get('workspace_handle')==dev_h2 and sc(r2).get('new_sha256')==edited_sha,'exact_retry_precedes_stale_handle_validation',call_state(r2))
r3=c.call('write_file',{'path':'mutable/should-not-write.txt','content':'bad','workspace_handle':dev_h,'operation_id':'pub-old-handle-newop-001'});expect(r3.get('isError') and state(r3)=='STALE_WORKSPACE_HANDLE','old_workspace_handle_rejected',call_state(r3))
# Same-handle race: exactly one side effect advances revision.
def race_call(i):
    cc=client();return cc.call('write_file',{'path':f'mutable/race{i}.txt','content':str(i),'workspace_handle':dev_h2,'operation_id':f'pub-race-{i}-001'})
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex: rr=list(ex.map(race_call,[1,2]))
succ=sum(bool(not x.get('isError')) for x in rr);stales=sum(bool(x.get('isError') and state(x)=='STALE_WORKSPACE_HANDLE') for x in rr)
expect(succ==1 and stales==1,'concurrent_same_handle_single_winner',[(x.get('isError'),state(x)) for x in rr])
# Fresh state.
p=reconcile(c,True);ns=nodes_by_name(p);dev_h=ns['dev']['workspace_handle'];audit_h=ns['audit']['workspace_handle']
# Persistent job launch exact retry, busy integrate/remove refused.
job_args={'workspace_handle':dev_h,'command':'sleep 30','timeout_ms':120000,'operation_id':'pub-job-001'}
r=c.call('start_workspace_command',job_args);expect(not r.get('isError'),'workspace_job_launch',call_state(r));job=sc(r);jid=job['job_id'];dev_h=job['workspace_handle']
rretry=c.call('start_workspace_command',job_args);expect(not rretry.get('isError') and sc(rretry).get('job_id')==jid and sc(rretry).get('workspace_handle')==dev_h,'workspace_job_exact_retry',call_state(rretry))
r=c.call('integrate_agent_workspace',{'workspace_handle':dev_h,'operation_id':'pub-integrate-busy-001'});expect(r.get('isError') and state(r)=='WORKSPACE_BUSY','integrate_busy_rejected',call_state(r))
r=c.call('remove_agent_workspace',{'workspace_handle':dev_h,'force':True,'reason':'busy test','operation_id':'pub-remove-busy-001'});expect(r.get('isError') and state(r)=='WORKSPACE_BUSY','retire_busy_rejected',call_state(r))
sr=c.call('stop_command',{'job_id':jid,'force':False,'operation_id':'pub-stop-busy-job-001'});expect(not sr.get('isError'),'stop_busy_test_job',call_state(sr))
st=poll(c,jid);expect(st.get('status')!='running','workspace_job_terminal_after_stop',st.get('status'))
# Verified producer.
producer_cmd="printf 'artifact\\n' > mutable/artifact.txt; printf '{\"passed\":true,\"failures\":[]}\\n' > mutable/test_status.json"
r=c.call('start_verified_workspace_producer',{'workspace_handle':dev_h,'pipeline':'test_pipeline','gate_path':'mutable/test_READY.json','result_json':'mutable/test_status.json','artifacts':['mutable/artifact.txt','mutable/test_status.json'],'command':producer_cmd,'expectations':{'passed':True},'metadata':{'test':True},'timeout_ms':120000,'operation_id':'pub-producer-001'});expect(not r.get('isError'),'verified_producer_launch',call_state(r));prod=sc(r);dev_h=prod['workspace_handle'];pst=poll(c,prod['job_id']);expect(pst.get('exit_code')==0,'verified_producer_terminal',pst.get('stderr','')[-500:])
# READY build is discoverable from reconcile.
p=reconcile(c,True);ns=nodes_by_name(p);devnode=ns['dev'];auditnode=ns['audit'];builds=[x for x in devnode.get('ready_builds',[]) if x.get('pipeline')=='test_pipeline'];expect(len(builds)>=1,'ready_build_discovered',builds);build=builds[0];dev_h=devnode['workspace_handle'];audit_h=auditnode['workspace_handle']
# Wrong build refused without consuming handles.
r=c.call('start_verified_workspace_consumer',{'workspace_handle':audit_h,'candidate_workspace_handle':dev_h,'candidate_build_id':'wrong-build','pipeline':'test_pipeline','gate_path':'mutable/test_READY.json','command':'true','operation_id':'pub-consumer-wrong-build-001'});expect(r.get('isError') and state(r)=='READY_BUILD_MISMATCH','wrong_ready_build_rejected',call_state(r))
# Correct audit binds exact build.
r=c.call('start_verified_workspace_consumer',{'workspace_handle':audit_h,'candidate_workspace_handle':dev_h,'candidate_build_id':build['build_id'],'pipeline':'test_pipeline','gate_path':'mutable/test_READY.json','required_artifacts':['mutable/artifact.txt'],'command':"printf 'audit-ok\\n' > mutable/audit.txt",'timeout_ms':120000,'operation_id':'pub-consumer-001'});expect(not r.get('isError'),'verified_consumer_launch',call_state(r));con=sc(r);audit_h=con['workspace_handle'];dev_h=con['candidate_workspace_handle'];cst=poll(c,con['job_id']);expect(cst.get('exit_code')==0,'verified_consumer_terminal',cst.get('stderr','')[-500:])
# Integrate producer, then UUID lookup gives durable integrated state and old handle cannot mutate.
r=c.call('integrate_agent_workspace',{'workspace_handle':dev_h,'operation_id':'pub-integrate-dev-001'});expect(not r.get('isError'),'producer_integration_success',call_state(r));token=sc(r)['project_state_token']
info=c.call('agent_workspace_info',{'workspace_id':dev_id});expect(not info.get('isError') and sc(info).get('control_state')=='integrated' and sc(info).get('status')=='integrated' and not sc(info).get('workspace_handle'),'integrated_uuid_tombstone',sc(info).get('control_state'))
p_hist=reconcile(c,True);hist=p_hist.get('recent_workspace_history',[]);devhist=next((x for x in hist if x.get('workspace_id')==dev_id),None);expect(devhist is not None and devhist.get('state')=='integrated' and any(b.get('pipeline')=='test_pipeline' for b in devhist.get('ready_builds',[])),'recent_history_preserves_integrated_ready_identity',devhist)
r=c.call('start_workspace_command',{'workspace_handle':dev_h,'command':'echo nope','operation_id':'pub-integrated-old-handle-001'});expect(r.get('isError') and state(r) in ('STALE_WORKSPACE_HANDLE','WORKSPACE_STATE_REJECTED'),'integrated_workspace_reuse_rejected',call_state(r))
# Audit workspace is dirty. Non-force retirement refuses, force preserves full physical workspace outside open namespace.
p=reconcile(c,True);ns=nodes_by_name(p);audit_h=ns['audit']['workspace_handle']
r=c.call('remove_agent_workspace',{'workspace_handle':audit_h,'force':False,'reason':'test','operation_id':'pub-retire-audit-noforce-001'});expect(r.get('isError') and state(r)=='dirty_retirement_refused','dirty_retirement_requires_force',call_state(r))
r=c.call('remove_agent_workspace',{'workspace_handle':audit_h,'force':True,'reason':'adversarial test retirement','operation_id':'pub-retire-audit-force-001'});expect(not r.get('isError'),'dirty_retirement_preserved',call_state(r));ret=sc(r);expect(Path(ret['preserved_at']).exists() and not (WROOT/audit_id).exists(),'dirty_retirement_physical_move',ret['preserved_at'])
info=c.call('agent_workspace_info',{'workspace_id':audit_id});expect(sc(info).get('state')=='retired' and not sc(info).get('workspace_handle'),'retired_uuid_history',sc(info).get('state'))
# Cold divergent workspace: create, modify, age, automatic maintenance suspend, then resume exactly.
p=reconcile(c);tok=p['project_state_token']
r=c.call('create_agent_workspace',{'project_path':str(PROJECT),'project_state_token':tok,'mutable_paths':['cold'],'name':'cold','operation_id':'pub-create-cold-001'});expect(not r.get('isError'),'create_cold_workspace',call_state(r));cold=sc(r);cold_id=cold['workspace_id'];cold_h=cold['workspace_handle']
r=c.call('edit_file',{'path':'cold/file.txt','target':'cold-base','replacement':'cold-changed','workspace_handle':cold_h,'operation_id':'pub-edit-cold-001'});expect(not r.get('isError'),'edit_cold_workspace',call_state(r));age_record(cold_id,180)
m=maintenance();expect(any(x['kind']=='suspend_divergent_idle' and x['workspace_id']==cold_id for x in m['actions']),'maintenance_suspends_divergent_idle',m['actions'])
expect(not (WROOT/cold_id).exists() and (REC/'workspace-suspended'/cold_id).exists(),'suspended_payload_moved',cold_id)
p=reconcile(c,True);coldnode=nodes_by_name(p)['cold'];expect(coldnode['state']=='suspended' and coldnode['workspace_handle'],'suspended_visible_in_reconcile',coldnode['state'])
r=c.call('resume_workspace',{'workspace_handle':coldnode['workspace_handle'],'project_state_token':p['project_state_token'],'operation_id':'pub-resume-cold-001'});expect(not r.get('isError'),'resume_suspended_workspace',call_state(r));expect((WROOT/cold_id/'work/cold/file.txt').read_text().strip()=='cold-changed','resume_preserves_divergent_content')
# Clean idle retirement.
p=reconcile(c);tok=p['project_state_token']
r=c.call('create_agent_workspace',{'project_path':str(PROJECT),'project_state_token':tok,'mutable_paths':['clean'],'name':'clean','operation_id':'pub-create-clean-001'});expect(not r.get('isError'),'create_clean_workspace',call_state(r));clean_id=sc(r)['workspace_id'];age_record(clean_id,180)
m=maintenance();expect(any(x['kind']=='retire_clean_idle' and x['workspace_id']==clean_id for x in m['actions']),'maintenance_retires_clean_idle',m['actions']);expect(not (WROOT/clean_id).exists(),'clean_idle_removed_from_open_namespace')
info=c.call('agent_workspace_info',{'workspace_id':clean_id});expect(sc(info).get('state')=='retired','clean_retired_history',sc(info).get('state'))
# Orphan quarantine.
orphan_id='bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb';od=WROOT/orphan_id/'work';od.mkdir(parents=True);(od/'orphan.txt').write_text('orphan')
m=maintenance();expect(any(x['kind']=='quarantine_orphan' and x['workspace_id']==orphan_id for x in m['actions']),'maintenance_quarantines_orphan',m['actions']);expect(not (WROOT/orphan_id).exists() and (REC/'workspace-orphans'/orphan_id/'work/orphan.txt').exists(),'orphan_payload_preserved')
# Maintenance vs short workspace execute race: execute holds topology lock until completion.
p=reconcile(c);tok=p['project_state_token']
r=c.call('create_agent_workspace',{'project_path':str(PROJECT),'project_state_token':tok,'mutable_paths':['other'],'name':'race-maint','operation_id':'pub-create-racemaint-001'});expect(not r.get('isError'),'create_maintenance_race_workspace',call_state(r));rm=sc(r);rm_id=rm['workspace_id'];rm_h=rm['workspace_handle']
resbox={}
def run_short():
    cc=client();resbox['execute']=cc.call('execute_command',{'workspace_handle':rm_h,'command':'sleep 2; printf done > other/race.txt','operation_id':'pub-short-race-001'})
th=threading.Thread(target=run_short);start=time.time();th.start();time.sleep(.2);m=maintenance();elapsed=time.time()-start;th.join(10);expect('execute' in resbox and not resbox['execute'].get('isError') and sc(resbox['execute']).get('exitCode')==0,'workspace_short_execute_success',call_state(resbox.get('execute',{})));safe_live_skip=any(x.get('reason') in ('live_job','live_job_after_lock') and x.get('workspace_id')==rm_id for x in m.get('skipped',[]));ok('maintenance_coordination_observed',{'elapsed':elapsed,'live_skip':safe_live_skip});expect((WROOT/rm_id).exists() and (WROOT/rm_id/'work/other/race.txt').read_text()=='done','maintenance_does_not_move_recent_command_workspace')
# Save current handle/token for restart-persistence test performed after server restart.
p=reconcile(c,True);node=nodes_by_name(p)['race-maint'];(T/'restart-state.json').write_text(json.dumps({'workspace_id':rm_id,'workspace_handle':node['workspace_handle'],'project_state_token':p['project_state_token']},indent=2))
# Final state checks.
expect((PROJECT/'mutable/file.txt').read_text().strip()=='changed','canonical_integration_content')
REPORT.write_text(json.dumps({'passed':True,'checks':checks},indent=2)+'\n')
print('ALL_PASS',len(checks))
