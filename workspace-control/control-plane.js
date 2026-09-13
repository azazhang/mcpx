const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().map((k) => `${JSON.stringify(k)}:${stableJson(value[k])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

function b64url(buf) { return Buffer.from(buf).toString('base64url'); }
function fromB64url(s) { return Buffer.from(String(s), 'base64url'); }
function nowIso() { return new Date().toISOString(); }
function sha256(v) { return crypto.createHash('sha256').update(v).digest('hex'); }

class WorkspaceControlError extends Error {
  constructor(code, message, extra = {}) {
    super(message);
    this.name = 'WorkspaceControlError';
    this.code = code;
    this.extra = extra;
  }
}

class WorkspaceControlPlane {
  constructor(opts) {
    const realOrResolve = (p) => { const r = path.resolve(p); try { return fs.realpathSync(r); } catch (_) { return r; } };
    this.workspaceRoot = realOrResolve(opts.workspaceRoot);
    this.recoveryDir = realOrResolve(opts.recoveryDir);
    this.serverVersion = opts.serverVersion || 'unknown';
    this.workspacePaths = opts.workspacePaths;
    this.readJson = opts.readJson;
    this.writeJson = opts.writeJson;
    this.workspaceChanged = opts.workspaceChanged;
    this.runningJobsForWorkspace = opts.runningJobsForWorkspace || (() => []);
    this.controlDir = path.join(this.recoveryDir, 'workspace-control');
    this.projectDir = path.join(this.recoveryDir, 'projects');
    this.workspaceRecordDir = path.join(this.recoveryDir, 'workspaces');
    this.lockDir = path.join(this.controlDir, 'locks');
    this.keyFile = path.join(this.controlDir, 'handle-key.bin');
    for (const d of [this.controlDir, this.projectDir, this.workspaceRecordDir, this.lockDir]) {
      fs.mkdirSync(d, { recursive: true, mode: 0o700 });
      try { fs.chmodSync(d, 0o700); } catch (_) {}
    }
    this.key = this._loadOrCreateKey();
  }

  _loadOrCreateKey() {
    try {
      const b = fs.readFileSync(this.keyFile);
      if (b.length >= 32) return b;
    } catch (_) {}
    const key = crypto.randomBytes(32);
    const tmp = `${this.keyFile}.tmp.${process.pid}.${crypto.randomUUID()}`;
    fs.writeFileSync(tmp, key, { mode: 0o600, flag: 'wx' });
    fs.renameSync(tmp, this.keyFile);
    try { fs.chmodSync(this.keyFile, 0o600); } catch (_) {}
    return key;
  }

  projectId(projectPath) { return sha256(path.resolve(projectPath)); }
  projectPath(id) { return path.join(this.projectDir, `${id}.json`); }
  workspaceRecordPath(id) { return path.join(this.workspaceRecordDir, `${id}.json`); }

  _sign(prefix, payload) {
    const encoded = b64url(Buffer.from(JSON.stringify(payload), 'utf8'));
    const mac = crypto.createHmac('sha256', this.key).update(`${prefix}.${encoded}`).digest();
    return `${prefix}.${encoded}.${b64url(mac)}`;
  }

  _verify(token, prefix) {
    const parts = String(token || '').split('.');
    if (parts.length !== 3 || parts[0] !== prefix) throw new WorkspaceControlError('INVALID_STATE_HANDLE', `Invalid ${prefix === 'w1' ? 'workspace handle' : 'project state token'}.`);
    let payload;
    try { payload = JSON.parse(fromB64url(parts[1]).toString('utf8')); }
    catch (_) { throw new WorkspaceControlError('INVALID_STATE_HANDLE', 'State token payload is invalid.'); }
    const expected = crypto.createHmac('sha256', this.key).update(`${prefix}.${parts[1]}`).digest();
    let got;
    try { got = fromB64url(parts[2]); } catch (_) { got = Buffer.alloc(0); }
    if (got.length !== expected.length || !crypto.timingSafeEqual(got, expected)) throw new WorkspaceControlError('INVALID_STATE_HANDLE', 'State token signature is invalid.');
    return payload;
  }

  loadProjectById(id) { return this.readJson(this.projectPath(id)); }
  loadWorkspace(id) { return this.readJson(this.workspaceRecordPath(id)); }

  _saveProject(rec) { this.writeJson(this.projectPath(rec.project_id), rec); return rec; }
  _saveWorkspace(rec) { this.writeJson(this.workspaceRecordPath(rec.workspace_id), rec); return rec; }

  _ensureProject(projectPath) {
    const canonical = path.resolve(projectPath);
    const id = this.projectId(canonical);
    let rec = this.loadProjectById(id);
    if (!rec) {
      rec = { project_id: id, project_path: canonical, topology_epoch: 1, workspace_ids: [], created_at: nowIso(), updated_at: nowIso() };
      this._saveProject(rec);
    } else if (rec.project_path !== canonical) {
      rec.project_path = canonical; rec.updated_at = nowIso(); this._saveProject(rec);
    }
    return rec;
  }

  _projectForWorkspace(rec) { return this._ensureProject(rec.project_path); }

  _bumpProject(project, reason, workspaceId = null) {
    const rec = typeof project === 'string' ? this._ensureProject(project) : project;
    rec.topology_epoch = Math.max(1, Number(rec.topology_epoch) || 1) + 1;
    rec.updated_at = nowIso();
    rec.last_topology_change = { at: rec.updated_at, reason, workspace_id: workspaceId };
    this._saveProject(rec);
    return rec;
  }

  _normalizeState(record, manifest) {
    const old = String(record?.state || '').toLowerCase();
    const manifestState = String(manifest?.status || '').toLowerCase();
    if (old === 'payload_garbage_collected') {
      const prior = String(record?.previous_state || '').toLowerCase();
      return ['integrated','retired'].includes(prior) ? prior : 'retired';
    }
    if (['retired_legacy_gced','retired_recent_obsolete_gced'].includes(old)) return 'retired';
    if (['retired','integrated','suspended'].includes(old)) return old;
    if (manifestState === 'integrated') return 'integrated';
    if (manifestState === 'open' || manifestState === 'active') return 'open';
    if (old === 'open' || old === 'active' || old.startsWith('active_')) return 'open';
    return old || 'open';
  }

  bootstrap() {
    const seenPhysical = new Set();
    let names = [];
    try { names = fs.readdirSync(this.workspaceRoot); } catch (_) {}
    for (const name of names) {
      if (!/^[0-9a-f-]{36}$/i.test(name)) continue;
      const wp = this.workspacePaths(name);
      const manifest = this.readJson(wp.manifest);
      if (!manifest) continue;
      if (String(manifest.status || '').toLowerCase() === 'active') {
        manifest.status = 'open';
        this.writeJson(wp.manifest, manifest);
      }
      seenPhysical.add(name);
      const existing = this.loadWorkspace(name);
      let rec = existing || {};
      const priorState = existing?.state ? this._normalizeState(existing, null) : null;
      const state = this._normalizeState(rec, manifest);
      const previousPhysical = !!rec.physical_present;
      const hadRecord = !!existing;
      rec = {
        ...rec,
        workspace_id: name,
        name: manifest.name ?? rec.name ?? null,
        project_path: manifest.project_path,
        mutable_paths: manifest.mutable_paths || rec.mutable_paths || [],
        created_at: manifest.created_at || rec.created_at || nowIso(),
        state,
        revision: Math.max(1, Number(rec.revision) || 1),
        physical_present: true,
        physical_workspace_path: wp.dir,
        server_version: manifest.server_version || rec.server_version || this.serverVersion,
        updated_at: rec.updated_at || manifest.created_at || nowIso(),
        last_reconciled_at: nowIso(),
      };
      const project = this._ensureProject(rec.project_path);
      rec.project_id = project.project_id;
      if (!project.workspace_ids.includes(name)) { project.workspace_ids.push(name); project.workspace_ids.sort(); this._saveProject(project); }
      this._saveWorkspace(rec);
      if (hadRecord && priorState && priorState !== state) this._bumpProject(project, 'workspace_state_reconciled', name);
      else if (state === 'open' && (!hadRecord || !previousPhysical)) this._bumpProject(project, hadRecord ? 'physical_workspace_reappeared' : 'physical_workspace_discovered', name);
    }

    // Import permanent records that predate this control plane. Historical records whose
    // project path points at another MCPX workspace remain forensic history but must never
    // become registered/current projects after migration.
    for (const file of fs.readdirSync(this.workspaceRecordDir).filter((x) => x.endsWith('.json'))) {
      const rec = this.readJson(path.join(this.workspaceRecordDir, file));
      if (!rec?.workspace_id) continue;
      const manifest = seenPhysical.has(rec.workspace_id) ? this.readJson(this.workspacePaths(rec.workspace_id).manifest) : null;
      const legacyState = String(rec.state || '').toLowerCase();
      const normalizedState = this._normalizeState(rec, manifest);
      if (legacyState !== normalizedState && !rec.legacy_state) rec.legacy_state = legacyState || null;
      if (legacyState === 'payload_garbage_collected' || legacyState === 'retired_legacy_gced' || legacyState === 'retired_recent_obsolete_gced') rec.payload_state = rec.payload_state || 'garbage_collected';
      if (!rec.project_path) {
        if (!rec.revision || rec.state !== normalizedState || legacyState === 'payload_garbage_collected' || legacyState === 'retired_legacy_gced' || legacyState === 'retired_recent_obsolete_gced') {
          rec.revision = Math.max(1, Number(rec.revision) || 1);
          rec.state = normalizedState;
          rec.last_reconciled_at = nowIso();
          this._saveWorkspace(rec);
        }
        continue;
      }
      const historicalNested = this.isHistoricalWorkspaceProjectPath(rec.project_path);
      let project = null;
      if (!historicalNested || ['open','suspended'].includes(normalizedState)) {
        project = this._ensureProject(rec.project_path);
        if (!project.workspace_ids.includes(rec.workspace_id)) { project.workspace_ids.push(rec.workspace_id); project.workspace_ids.sort(); this._saveProject(project); }
      }
      if (!rec.revision || (project && !rec.project_id) || rec.state !== normalizedState || rec.physical_present !== seenPhysical.has(rec.workspace_id)) {
        if (project) rec.project_id = project.project_id;
        rec.revision = Math.max(1, Number(rec.revision) || 1);
        rec.state = normalizedState;
        rec.physical_present = seenPhysical.has(rec.workspace_id);
        rec.last_reconciled_at = nowIso();
        this._saveWorkspace(rec);
      }
    }
    return { physical_workspaces: seenPhysical.size, projects: fs.readdirSync(this.projectDir).filter((x)=>x.endsWith('.json')).length };
  }

  projectToken(project) {
    return this._sign('p1', { project_id: project.project_id, epoch: Number(project.topology_epoch) || 1, project_path: project.project_path });
  }

  currentProjectToken(projectPath) { return this.projectToken(this._ensureProject(projectPath)); }
  decodeWorkspaceHandle(handle) { return this._verify(handle, 'w1'); }
  pathsOverlap(a,b) { return this._pathsOverlap(a,b); }

  workspaceHandle(record) {
    const project = this._ensureProject(record.project_path);
    return this._sign('w1', { workspace_id: record.workspace_id, revision: Number(record.revision) || 1, project_id: project.project_id, state: record.state });
  }

  validateProjectToken(token, expectedPath = null) {
    const p = this._verify(token, 'p1');
    const rec = this.loadProjectById(p.project_id);
    if (!rec) throw new WorkspaceControlError('STALE_PROJECT_STATE', 'Project state token refers to an unknown project. Reconcile project state again.');
    if (expectedPath && path.resolve(expectedPath) !== path.resolve(rec.project_path)) throw new WorkspaceControlError('PROJECT_TOKEN_MISMATCH', 'Project state token does not match project_path.');
    // Topology epochs are observational. Correctness is enforced by server-side scope locks
    // and atomic overlap checks, so unrelated workspace activity must not stale a caller.
    return rec;
  }

  validateWorkspaceHandle(handle, allowedStates = ['open']) {
    const h = this._verify(handle, 'w1');
    const rec = this.loadWorkspace(h.workspace_id);
    if (!rec) throw new WorkspaceControlError('UNKNOWN_WORKSPACE', `Workspace ${h.workspace_id} has no durable control-plane record.`);
    const project = this._ensureProject(rec.project_path);
    if (h.project_id !== project.project_id) throw new WorkspaceControlError('STALE_WORKSPACE_HANDLE', 'Workspace project identity changed. Reconcile project state again.', { workspace_id: rec.workspace_id });
    if (Number(h.revision) !== Number(rec.revision)) throw new WorkspaceControlError('STALE_WORKSPACE_HANDLE', `Workspace revision changed (handle=${h.revision}, current=${rec.revision}). Reconcile project state again.`, { workspace_id: rec.workspace_id, current_revision: rec.revision });
    const allowed = (allowedStates || []).map((x) => x === 'active' ? 'open' : x);
    if (!allowed.includes(rec.state)) throw new WorkspaceControlError('WORKSPACE_STATE_REJECTED', `Workspace ${rec.workspace_id} state=${rec.state}; allowed=${allowed.join(',')}.`, { workspace_id: rec.workspace_id, state: rec.state, successor_workspace_id: rec.successor_workspace_id || null });
    if (rec.state === 'open') {
      const manifest = this.readJson(this.workspacePaths(rec.workspace_id).manifest);
      if (!manifest) throw new WorkspaceControlError('WORKSPACE_PHYSICAL_STATE_MISSING', `Open workspace ${rec.workspace_id} has no physical manifest. Reconcile/maintenance repair is required.`);
    }
    return { record: rec, project, payload: h };
  }

  async _acquireLock(kind, key, waitMs = 30000) {
    const dir = path.join(this.lockDir, kind, sha256(String(key)));
    const owner = path.join(dir, 'owner.json');
    const deadline = Date.now() + waitMs;
    fs.mkdirSync(path.dirname(dir), { recursive: true, mode: 0o700 });
    while (true) {
      try {
        await fs.promises.mkdir(dir, { mode: 0o700 });
        this.writeJson(owner, { pid: process.pid, kind, key, created_at: nowIso() });
        return async () => { await fs.promises.rm(dir, { recursive: true, force: true }); };
      } catch (err) {
        if (err.code !== 'EEXIST') throw err;
        const old = this.readJson(owner); let alive = false;
        if (old?.pid) { try { process.kill(Number(old.pid), 0); alive = true; } catch (_) {} }
        if (!alive) { await fs.promises.rm(dir, { recursive: true, force: true }); continue; }
        if (Date.now() >= deadline) throw new WorkspaceControlError('WORKSPACE_LOCK_BUSY', `${kind} lock is held for ${key}.`, { owner: old });
        await new Promise((r) => setTimeout(r, 50));
      }
    }
  }

  acquireProjectLock(projectPath, waitMs) { return this._acquireLock('project', path.resolve(projectPath), waitMs); }
  acquireTopologyLock(waitMs) { return this._acquireLock('topology', 'global-workspace-topology', waitMs); }
  acquireIntegrationLock(waitMs) { return this._acquireLock('integration', 'global-canonical-integration', waitMs); }
  acquireWorkspaceLock(workspaceId, waitMs) { return this._acquireLock('workspace', workspaceId, waitMs); }

  async consumeWorkspaceHandle(handle, allowedStates = ['open'], reason = 'workspace_side_effect') {
    const initial = this._verify(handle, 'w1');
    const release = await this.acquireWorkspaceLock(initial.workspace_id);
    try {
      const { record, project } = this.validateWorkspaceHandle(handle, allowedStates);
      record.revision = Math.max(1, Number(record.revision) || 1) + 1;
      record.updated_at = nowIso();
      record.last_revision_reason = reason;
      this._saveWorkspace(record);
      return { record, project, next_handle: this.workspaceHandle(record) };
    } finally { await release(); }
  }

  _pathsOverlap(a, b) {
    const norm = (x) => path.normalize(String(x)).replace(/\\/g, '/').replace(/^\.\//,'').replace(/\/$/,'');
    for (const x0 of a || []) for (const y0 of b || []) {
      const x = norm(x0), y = norm(y0);
      if (x === y || x.startsWith(`${y}/`) || y.startsWith(`${x}/`)) return true;
    }
    return false;
  }

  canonicalMutablePaths(projectPath, mutablePaths) {
    let root = path.resolve(projectPath); try { root = fs.realpathSync(root); } catch (_) {}
    return (mutablePaths || []).map((raw) => path.resolve(root, String(raw)));
  }

  _absolutePathsOverlap(a, b) {
    const norm = (x) => path.resolve(String(x));
    for (const x0 of a || []) for (const y0 of b || []) {
      const x = norm(x0), y = norm(y0);
      if (x === y || x.startsWith(`${y}${path.sep}`) || y.startsWith(`${x}${path.sep}`)) return true;
    }
    return false;
  }

  canonicalPathsOverlap(projectA, pathsA, projectB, pathsB) {
    return this._absolutePathsOverlap(this.canonicalMutablePaths(projectA, pathsA), this.canonicalMutablePaths(projectB, pathsB));
  }

  _recordRelevantToScope(rec, scopePath) {
    if (!scopePath) return true;
    return this._absolutePathsOverlap(this.canonicalMutablePaths(rec.project_path, rec.mutable_paths), [path.resolve(scopePath)]);
  }

  openRecords(scopePath = null) {
    const out = [];
    for (const file of fs.readdirSync(this.workspaceRecordDir).filter((x)=>x.endsWith('.json'))) {
      const rec = this.readJson(path.join(this.workspaceRecordDir, file));
      if (!rec || !['open','suspended'].includes(this._normalizeState(rec, null))) continue;
      rec.state = this._normalizeState(rec, null);
      if (this._recordRelevantToScope(rec, scopePath)) out.push(rec);
    }
    return out;
  }

  validateCreateRequest({ project_path, project_state_token, mutable_paths, branch_of_handle = null, branch_role = null, branch_reason = null }) {
    const project = this.validateProjectToken(project_state_token, project_path);
    const overlapping = this.openRecords().filter((r) => this.canonicalPathsOverlap(project_path, mutable_paths, r.project_path, r.mutable_paths));
    if (!overlapping.length) return { project, overlapping: [], branch_parent: null };
    if (!branch_of_handle || !branch_role || !['independent_audit','independent_branch'].includes(branch_role) || !String(branch_reason || '').trim()) {
      throw new WorkspaceControlError('OPEN_WORKSPACE_OVERLAP', `Requested canonical mutable scope overlaps ${overlapping.length} open/suspended workspace(s). Reuse/finish the existing work packet, or explicitly declare an independent audit/branch.`, { overlapping: overlapping.map((r)=>({workspace_id:r.workspace_id,name:r.name,state:r.state,project_path:r.project_path,mutable_paths:r.mutable_paths})) });
    }
    const parent = this.validateWorkspaceHandle(branch_of_handle, ['open','suspended']).record;
    if (!overlapping.some((r)=>r.workspace_id===parent.workspace_id)) throw new WorkspaceControlError('BRANCH_PARENT_NOT_OVERLAPPING', 'branch_of_handle is not one of the physically overlapping workspaces.');
    return { project, overlapping, branch_parent: parent };
  }

  registerCreated(manifest, { branch_parent = null, branch_role = null, branch_reason = null } = {}) {
    const project = this._ensureProject(manifest.project_path);
    const rec = {
      workspace_id: manifest.id,
      name: manifest.name || null,
      project_id: project.project_id,
      project_path: manifest.project_path,
      mutable_paths: manifest.mutable_paths || [],
      created_at: manifest.created_at || nowIso(),
      state: 'open',
      revision: 1,
      physical_present: true,
      physical_workspace_path: this.workspacePaths(manifest.id).dir,
      role: branch_role || 'development',
      branch_parent_workspace_id: branch_parent?.workspace_id || null,
      branch_reason: branch_reason || null,
      updated_at: nowIso(),
      server_version: this.serverVersion,
    };
    this._saveWorkspace(rec);
    if (!project.workspace_ids.includes(rec.workspace_id)) project.workspace_ids.push(rec.workspace_id);
    project.workspace_ids.sort(); this._bumpProject(project, 'workspace_created', rec.workspace_id);
    const currentProject = this._ensureProject(manifest.project_path);
    return { record: rec, workspace_handle: this.workspaceHandle(rec), project_state_token: this.projectToken(currentProject) };
  }

  _captureReadyBuilds(rec, key) {
    try {
      const builds = this._readyBuilds(rec);
      if (builds.length) rec[key] = builds;
    } catch (_) {}
  }

  markIntegrated(workspaceId, integration = {}) {
    const rec = this.loadWorkspace(workspaceId); if (!rec) throw new WorkspaceControlError('UNKNOWN_WORKSPACE', `Unknown workspace ${workspaceId}`);
    if (rec.state === 'open') this._captureReadyBuilds(rec, 'ready_builds_at_close');
    rec.state = 'integrated'; rec.integrated_at = integration.integrated_at || nowIso(); rec.integration = integration; rec.revision = Math.max(1, Number(rec.revision)||1)+1; rec.updated_at = nowIso(); this._saveWorkspace(rec);
    const project = this._bumpProject(rec.project_path, 'workspace_integrated', workspaceId);
    return { record: rec, project_state_token: this.projectToken(project) };
  }

  markRetired(workspaceId, reason, extra = {}) {
    const rec = this.loadWorkspace(workspaceId); if (!rec) throw new WorkspaceControlError('UNKNOWN_WORKSPACE', `Unknown workspace ${workspaceId}`);
    if (rec.state === 'open') this._captureReadyBuilds(rec, 'ready_builds_at_close');
    rec.state = extra.state || 'retired'; rec.retired_at = nowIso(); rec.retirement_reason = reason; rec.physical_present = extra.physical_present ?? false; rec.revision = Math.max(1, Number(rec.revision)||1)+1; rec.updated_at = nowIso(); Object.assign(rec, extra); this._saveWorkspace(rec);
    const project = this._bumpProject(rec.project_path, 'workspace_retired', workspaceId);
    return { record: rec, project_state_token: this.projectToken(project) };
  }

  markPayloadGarbageCollected(workspaceId, extra = {}) {
    const rec = this.loadWorkspace(workspaceId); if (!rec) throw new WorkspaceControlError('UNKNOWN_WORKSPACE', `Unknown workspace ${workspaceId}`);
    rec.payload_state = 'garbage_collected'; rec.payload_garbage_collected_at = nowIso(); rec.physical_present = extra.physical_present ?? false; rec.revision = Math.max(1, Number(rec.revision)||1)+1; rec.updated_at = nowIso(); Object.assign(rec, extra); this._saveWorkspace(rec);
    const project = this._bumpProject(rec.project_path, 'workspace_payload_garbage_collected', workspaceId);
    return { record: rec, project_state_token: this.projectToken(project) };
  }

  markSuspended(workspaceId, archiveMeta = {}) {
    const rec = this.loadWorkspace(workspaceId); if (!rec) throw new WorkspaceControlError('UNKNOWN_WORKSPACE', `Unknown workspace ${workspaceId}`);
    if (rec.state === 'open') this._captureReadyBuilds(rec, 'ready_builds_at_suspend');
    rec.state = 'suspended'; rec.suspended_at = nowIso(); rec.suspension = archiveMeta; rec.physical_present = archiveMeta.physical_present ?? false; rec.revision = Math.max(1, Number(rec.revision)||1)+1; rec.updated_at = nowIso(); this._saveWorkspace(rec);
    const project = this._bumpProject(rec.project_path, 'workspace_suspended', workspaceId);
    return { record: rec, project_state_token: this.projectToken(project), workspace_handle: this.workspaceHandle(rec) };
  }

  finalizePhysicalState(workspaceId, extra = {}) {
    const rec = this.loadWorkspace(workspaceId); if (!rec) throw new WorkspaceControlError('UNKNOWN_WORKSPACE', `Unknown workspace ${workspaceId}`);
    Object.assign(rec, extra); rec.updated_at = nowIso(); this._saveWorkspace(rec); return rec;
  }

  markResumed(workspaceId) {
    const rec = this.loadWorkspace(workspaceId); if (!rec) throw new WorkspaceControlError('UNKNOWN_WORKSPACE', `Unknown workspace ${workspaceId}`);
    rec.state = 'open'; rec.resumed_at = nowIso(); rec.physical_present = true; rec.resume_pending = null; rec.revision = Math.max(1, Number(rec.revision)||1)+1; rec.updated_at = nowIso(); this._saveWorkspace(rec);
    const project = this._bumpProject(rec.project_path, 'workspace_resumed', workspaceId);
    return { record: rec, project_state_token: this.projectToken(project), workspace_handle: this.workspaceHandle(rec) };
  }

  _readyBuilds(rec, maxFiles = 5000, maxBuilds = 20) {
    if (rec.state !== 'open') return [];
    const root = this.workspacePaths(rec.workspace_id).work;
    const builds = []; let scanned = 0;
    const seen = new Set();
    const visit = (rel) => {
      if (scanned >= maxFiles || builds.length >= maxBuilds) return;
      const full = path.join(root, rel);
      let st; try { st = fs.lstatSync(full); } catch (_) { return; }
      if (st.isSymbolicLink()) return;
      if (st.isDirectory()) {
        let names=[]; try { names=fs.readdirSync(full); } catch (_) { return; }
        for (const n of names) { if (scanned >= maxFiles || builds.length >= maxBuilds) break; visit(path.join(rel,n)); }
        return;
      }
      if (!st.isFile()) return; scanned += 1;
      const base=path.basename(rel);
      if (!/(?:^READY\.json$|_READY\.json$|VERIFIED_READY\.json$)/i.test(base)) return;
      let j; try { j=JSON.parse(fs.readFileSync(full,'utf8')); } catch (_) { return; }
      if (j?.state !== 'READY' || !j?.build_id || !j?.pipeline) return;
      const key=`${j.pipeline}|${j.build_id}|${rel}`; if (seen.has(key)) return; seen.add(key);
      builds.push({pipeline:j.pipeline,build_id:j.build_id,gate_path:String(rel).replace(/\\/g,'/'),artifact_count:Array.isArray(j.artifacts)?j.artifacts.length:(j.artifact_count??null),created_at:j.created_at||j.ready_at||null});
    };
    for (const m of rec.mutable_paths || []) { if (builds.length >= maxBuilds || scanned >= maxFiles) break; visit(m); }
    return builds.sort((a,b)=>String(b.created_at||'').localeCompare(String(a.created_at||'')) || String(a.pipeline).localeCompare(String(b.pipeline)));
  }

  _scopeRelation(projectPath, scopePath) {
    if (!scopePath) return null;
    const p = path.resolve(projectPath), s = path.resolve(scopePath);
    if (p === s) return 'same_root';
    if (s.startsWith(`${p}${path.sep}`)) return 'ancestor_root';
    if (p.startsWith(`${s}${path.sep}`)) return 'descendant_root';
    return 'cross_root_scope';
  }

  async _workspaceSummary(rec, detail = false, scopePath = null) {
    const physical = rec.state === 'open' && !!this.readJson(this.workspacePaths(rec.workspace_id).manifest);
    let changed = Array.isArray(rec.changed_paths) ? rec.changed_paths : null;
    if (detail && physical && this.workspaceChanged) {
      try { changed = await this.workspaceChanged(this.readJson(this.workspacePaths(rec.workspace_id).manifest)); }
      catch (_) {}
    }
    let running = [];
    try { running = this.runningJobsForWorkspace(rec.workspace_id) || []; } catch (_) {}
    const ready = rec.state === 'open' ? this._readyBuilds(rec) : (rec.ready_builds_at_suspend || []);
    return {
      workspace_id: rec.workspace_id,
      name: rec.name || null,
      state: rec.state,
      role: rec.role || null,
      workspace_project_path: rec.project_path,
      scope_relation: this._scopeRelation(rec.project_path, scopePath),
      branch_parent_workspace_id: rec.branch_parent_workspace_id || null,
      created_at: rec.created_at || null,
      updated_at: rec.updated_at || null,
      revision: rec.revision || 1,
      mutable_paths: rec.mutable_paths || [],
      changed_count: changed ? changed.length : (rec.changed_count ?? null),
      changed_paths: detail ? (changed || []) : undefined,
      physical_present: physical,
      running_jobs: running.map((j)=>({job_id:j.job_id,status:j.status,operation_id:j.operation_id||null,started_at:j.started_at||null})),
      ready_builds: ready,
      successor_workspace_id: rec.successor_workspace_id || null,
      integrated_at: rec.integrated_at || null,
      suspended_at: rec.suspended_at || null,
      workspace_handle: ['open','suspended'].includes(rec.state) ? this.workspaceHandle(rec) : null,
    };
  }

  _terminalHistorySummary(rec, scopePath = null) {
    return {
      workspace_id: rec.workspace_id,
      name: rec.name || null,
      state: rec.state,
      role: rec.role || null,
      workspace_project_path: rec.project_path,
      scope_relation: this._scopeRelation(rec.project_path, scopePath),
      created_at: rec.created_at || null,
      updated_at: rec.updated_at || null,
      completed_at: rec.integrated_at || rec.retired_at || rec.updated_at || rec.created_at || null,
      integrated_at: rec.integrated_at || null,
      retired_at: rec.retired_at || null,
      retirement_reason: rec.retirement_reason || null,
      mutable_path_count: (rec.mutable_paths || []).length,
      mutable_paths_preview: (rec.mutable_paths || []).slice(0, 3),
      ready_builds: (rec.ready_builds_at_close || []).slice(0, 5),
      payload_state: rec.payload_state || null,
      successor_workspace_id: rec.successor_workspace_id || null,
      last_revision_reason: rec.last_revision_reason || null,
    };
  }

  _terminalRecords(scopePath = null, limit = 12, exactProject = false) {
    const out = [];
    for (const file of fs.readdirSync(this.workspaceRecordDir).filter((x)=>x.endsWith('.json'))) {
      const rec = this.readJson(path.join(this.workspaceRecordDir, file));
      if (!rec) continue;
      rec.state = this._normalizeState(rec, null);
      if (['open','suspended'].includes(rec.state)) continue;
      if (scopePath && !rec.project_path) continue;
      if (scopePath) {
        if (exactProject) {
          if (path.resolve(rec.project_path || '') !== path.resolve(scopePath)) continue;
        } else if (!this._recordRelevantToScope(rec, scopePath)) continue;
      }
      out.push(rec);
    }
    out.sort((a,b)=>String(b.integrated_at||b.retired_at||b.updated_at||b.created_at||'').localeCompare(String(a.integrated_at||a.retired_at||a.updated_at||a.created_at||'')));
    return Number.isFinite(limit) ? out.slice(0, Math.max(0, limit)) : out;
  }

  async reconcileProjectState(projectPath = null, detail = false) {
    this.bootstrap();
    const reconciledAt = nowIso();
    if (projectPath) {
      let canonical = path.resolve(projectPath);
      try { canonical = fs.realpathSync(canonical); } catch (_) {}
      const p = this._ensureProject(canonical);
      const openRecords = this.openRecords(canonical);
      const nodes = [];
      for (const rec of openRecords) nodes.push(await this._workspaceSummary(rec, detail, canonical));
      nodes.sort((a,b)=>String(b.updated_at||b.created_at||'').localeCompare(String(a.updated_at||a.created_at||'')));
      const allClosed = this._terminalRecords(canonical, Number.POSITIVE_INFINITY, false);
      const recentIntegrated = allClosed.filter((r)=>r.state==='integrated').slice(0, 8);
      const recentOtherClosed = allClosed.filter((r)=>r.state!=='integrated').slice(0, 2);
      const history = [...recentIntegrated,...recentOtherClosed].map((r)=>this._terminalHistorySummary(r, canonical));
      return { server_version:this.serverVersion, reconciled_at:reconciledAt, projects:[{
        project_id:p.project_id,
        project_path:p.project_path,
        topology_epoch:p.topology_epoch,
        project_state_token:this.projectToken(p),
        open_or_suspended_workspaces:nodes,
        recent_workspace_history:history,
        closed_workspace_count:allClosed.length,
        updated_at:p.updated_at,
      }] };
    }

    const projects = fs.readdirSync(this.projectDir).filter((x)=>x.endsWith('.json')).map((x)=>this.readJson(path.join(this.projectDir,x))).filter(Boolean);
    projects.sort((a,b)=>String(b.updated_at||'').localeCompare(String(a.updated_at||'')));
    const allOpen = this.openRecords();
    const out = [];
    for (const p of projects) {
      const exact = allOpen.filter((r)=>path.resolve(r.project_path||'')===path.resolve(p.project_path));
      const nodes=[]; for (const rec of exact) nodes.push(await this._workspaceSummary(rec, detail, p.project_path));
      nodes.sort((a,b)=>String(b.updated_at||b.created_at||'').localeCompare(String(a.updated_at||a.created_at||'')));
      const allClosed = this._terminalRecords(p.project_path, Number.POSITIVE_INFINITY, true);
      const recentIntegrated = allClosed.filter((r)=>r.state==='integrated').slice(0, 6);
      const recentOtherClosed = allClosed.filter((r)=>r.state!=='integrated').slice(0, 2);
      const history = [...recentIntegrated,...recentOtherClosed].map((r)=>this._terminalHistorySummary(r, p.project_path));
      if (nodes.length || history.length) out.push({ project_id:p.project_id, project_path:p.project_path, topology_epoch:p.topology_epoch, project_state_token:this.projectToken(p), open_or_suspended_workspaces:nodes, recent_workspace_history:history, closed_workspace_count:allClosed.length, updated_at:p.updated_at });
    }
    return { server_version:this.serverVersion, reconciled_at:reconciledAt, projects:out };
  }

  tombstoneInfo(id) {
    const rec = this.loadWorkspace(id); if (!rec) return null;
    return { ...rec, workspace_handle: ['open','suspended'].includes(rec.state) ? this.workspaceHandle(rec) : null };
  }

  isWorkspaceRootPath(p) {
    const full = path.resolve(p); const rel = path.relative(this.workspaceRoot, full);
    return rel === '' || (!rel.startsWith('..') && !path.isAbsolute(rel));
  }

  isHistoricalWorkspaceProjectPath(p) {
    const full = path.resolve(String(p || ''));
    if (this.isWorkspaceRootPath(full)) return true;
    return /(?:^|\/)mcpx-workspaces\/[0-9a-f-]{36}\/work(?:\/|$)/i.test(full.replace(/\\/g,'/'));
  }

  isMutablePath(record, rel) {
    const target = path.normalize(String(rel)).replace(/\\/g,'/').replace(/^\.\//,'');
    return (record.mutable_paths || []).some((raw) => {
      const m = path.normalize(String(raw)).replace(/\\/g,'/').replace(/^\.\//,'').replace(/\/$/,'');
      return target === m || target.startsWith(`${m}/`);
    });
  }

  resolveWorkspaceRelative(handle, rel, allowedStates = ['open'], requireMutable = false) {
    const { record } = this.validateWorkspaceHandle(handle, allowedStates);
    if (typeof rel !== 'string' || !rel.trim() || path.isAbsolute(rel)) throw new WorkspaceControlError('WORKSPACE_PATH_INVALID', 'Workspace-aware path must be a non-empty project-relative path.');
    const n = path.normalize(rel).replace(/^\.\//,'');
    if (n === '..' || n.startsWith(`..${path.sep}`)) throw new WorkspaceControlError('WORKSPACE_PATH_INVALID', 'Workspace-aware path escapes the workspace.');
    if (requireMutable && !this.isMutablePath(record, n)) throw new WorkspaceControlError('WORKSPACE_PATH_READ_ONLY', `Path ${n} is outside the workspace mutable paths.`, { workspace_id:record.workspace_id, mutable_paths:record.mutable_paths });
    return { record, fullPath:path.join(this.workspacePaths(record.workspace_id).work,n), relativePath:n };
  }
}

module.exports = { WorkspaceControlPlane, WorkspaceControlError, stableJson };
