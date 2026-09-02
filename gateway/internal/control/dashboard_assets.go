package control

const dashboardHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>IndexQube</title>
  <style>
    :root { color-scheme: dark; --bg:#0b1020; --panel:#121a2d; --line:#26334d; --text:#e9eefb; --muted:#96a3ba; --accent:#9b8cff; --good:#67d6a4; --warn:#f2bf68; --bad:#ff7e8a; }
    * { box-sizing:border-box } body { margin:0; background:var(--bg); color:var(--text); font:14px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace }
    button,input,select,textarea { font:inherit } button { cursor:pointer }
    header { height:58px; display:flex; align-items:center; justify-content:space-between; padding:0 20px; border-bottom:1px solid var(--line); background:#0e1527 }
    h1 { font:700 18px/1 system-ui,sans-serif; margin:0 } #health { color:var(--muted); font-size:12px }
    main { display:grid; grid-template-columns:310px minmax(0,1fr); min-height:calc(100vh - 58px) }
    aside { border-right:1px solid var(--line); padding:14px; overflow:auto } .content { padding:18px; overflow:auto }
    .label { color:var(--muted); font-size:11px; letter-spacing:.12em; text-transform:uppercase; margin:12px 0 7px }
    .task { width:100%; text-align:left; color:var(--text); background:transparent; border:1px solid transparent; border-radius:8px; padding:10px; margin:0 0 5px }
    .task:hover,.task.active { background:#18223a; border-color:var(--line) } .task .meta { color:var(--muted); font-size:11px; margin-top:4px }
    .pill { display:inline-block; border:1px solid var(--line); border-radius:999px; padding:2px 7px; margin-right:5px; color:var(--muted); font-size:11px }
    .pill.open,.pill.succeeded,.pill.available,.pill.verified { color:var(--good) } .pill.running,.pill.awaiting_approval,.pill.warning { color:var(--warn) } .pill.needs_attention,.pill.failed,.pill.incompatible,.pill.unavailable { color:var(--bad) }
    .panel { background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:14px; margin:0 0 14px }
    .grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:14px } h2,h3 { font-family:system-ui,sans-serif; margin:0 0 10px } h2 { font-size:19px } h3 { font-size:14px }
    .turn { border-left:2px solid var(--line); padding:4px 0 8px 12px; margin:8px 0 } .turn .who { color:var(--accent); font-size:11px; text-transform:uppercase }
    pre { white-space:pre-wrap; overflow-wrap:anywhere; margin:5px 0 0; color:var(--text); font:inherit }
    .row { border-top:1px solid var(--line); padding:8px 0; overflow-wrap:anywhere } .row:first-child { border-top:0 }
    form { display:flex; gap:8px; align-items:flex-end; flex-wrap:wrap } textarea,input,select { background:#0b1222; color:var(--text); border:1px solid var(--line); border-radius:7px; padding:8px }
    textarea { width:100%; min-height:64px; resize:vertical } .grow { flex:1 1 420px } label { color:var(--muted); font-size:12px }
    button.action { border:1px solid var(--line); background:#202b46; color:var(--text); border-radius:7px; padding:8px 11px } button.primary { background:#6356d8; border-color:#8175ed }
    button.danger { color:#ffc1c6 } #status { position:fixed; right:16px; bottom:14px; max-width:min(620px,90vw); background:#111a2e; border:1px solid var(--line); border-radius:8px; padding:8px 11px; color:var(--muted) }
    .empty { color:var(--muted) } .finding { color:var(--warn) }
    @media(max-width:850px) { main { grid-template-columns:1fr } aside { border-right:0; border-bottom:1px solid var(--line); max-height:270px } .grid { grid-template-columns:1fr } }
  </style>
</head>
<body>
  <header><h1>IndexQube</h1><div id="health">Connecting…</div></header>
  <main>
    <aside>
      <div class="label">Tasks</div><div id="tasks"></div>
      <div class="label">New task</div>
      <form id="new-form">
        <textarea id="new-prompt" aria-label="New task request" placeholder="What should IndexQube do?"></textarea>
        <select id="new-backend" aria-label="Backend"><option value="">Auto</option><option>codex</option><option>claude</option></select>
        <label><input id="new-write" type="checkbox"> allow workspace writes</label>
        <button class="action primary" type="submit">Start</button>
      </form>
    </aside>
    <section class="content">
      <div id="task-header" class="panel"></div>
      <div id="approvals"></div>
      <div class="panel"><h3>Continue conversation</h3><form id="continue-form"><div class="grow"><textarea id="continue-prompt" aria-label="Continue task" placeholder="Send the next request…"></textarea></div><button class="action primary" type="submit">Send</button></form></div>
      <div class="grid"><div id="conversation" class="panel"></div><div id="verification" class="panel"></div></div>
      <div class="grid"><div id="files" class="panel"></div><div id="commands" class="panel"></div></div>
      <div class="grid"><div id="routes" class="panel"></div><div id="handoffs" class="panel"></div></div>
    </section>
  </main>
  <div id="status">Dashboard session is local to this daemon.</div>
  <script src="/control/ui/app.js" defer></script>
</body>
</html>`

const dashboardJS = `'use strict';
const byId = id => document.getElementById(id);
const state = { tasks: [], approvals: [], backends: [], selected: '', evidence: null, workspace: '' };

async function api(path, options = {}) {
  const headers = new Headers(options.headers || {});
  headers.set('X-IndexQube-Dashboard', '1');
  if (options.body && typeof options.body !== 'string') {
    headers.set('Content-Type', 'application/json');
    options.body = JSON.stringify(options.body);
  }
  const response = await fetch(path, { credentials: 'same-origin', ...options, headers });
  const text = await response.text();
  let payload = null;
  if (text) { try { payload = JSON.parse(text); } catch (_) { payload = null; } }
  if (!response.ok) throw new Error(payload?.error || response.statusText || 'request failed');
  return payload;
}

function node(tag, text, className) {
  const element = document.createElement(tag);
  if (text !== undefined && text !== null) element.textContent = String(text);
  if (className) element.className = className;
  return element;
}

function clear(id, heading) {
  const root = byId(id); root.replaceChildren();
  if (heading) root.append(node('h3', heading));
  return root;
}

function short(value) { return value && value.length > 18 ? value.slice(0, 18) + '…' : (value || ''); }
function status(message) { byId('status').textContent = message; }
function pill(text, kind) { return node('span', text, 'pill ' + (kind || '')); }

function renderHealth() {
  const root = byId('health'); root.replaceChildren();
  state.backends.filter(item => item.backend !== 'fake').forEach(item => {
    root.append(pill(item.backend + ' ' + item.status + (item.version ? ' · ' + item.version : ''), item.status));
  });
}

function renderTasks() {
  const root = clear('tasks');
  if (!state.tasks.length) { root.append(node('div', 'No tasks yet.', 'empty')); return; }
  state.tasks.forEach(task => {
    const button = node('button', null, 'task' + (task.task_id === state.selected ? ' active' : ''));
    button.type = 'button';
    button.append(node('div', task.original_goal || '(untitled)'));
    const meta = node('div', null, 'meta');
    meta.append(pill(task.status, task.status), pill(task.preferred_backend), document.createTextNode(' ' + short(task.task_id)));
    button.append(meta);
    button.addEventListener('click', () => selectTask(task.task_id));
    root.append(button);
  });
}

function row(root, primary, secondary, className) {
  const item = node('div', null, 'row' + (className ? ' ' + className : ''));
  item.append(node('div', primary));
  if (secondary) item.append(node('pre', secondary, 'empty'));
  root.append(item);
}

function renderEvidence() {
  const evidence = state.evidence;
  const header = clear('task-header');
  for (const section of ['conversation','verification','files','commands','routes','handoffs']) clear(section, section[0].toUpperCase() + section.slice(1));
  byId('continue-form').closest('.panel').style.display = evidence ? '' : 'none';
  if (!evidence) { header.append(node('div', 'Select a task or start a new one.', 'empty')); return; }
  const task = evidence.task;
  header.append(node('h2', task.original_goal));
  header.append(pill(task.status, task.status), pill(task.preferred_backend), pill(task.permission));
  header.append(node('div', task.workspace_path + ' · ' + task.task_id, 'empty'));
  const actions = node('div'); actions.style.marginTop = '10px';
  [['Cancel','cancel','danger'],['Reopen','reopen',''],['Close','close',''],['Pin','pin',''],['Unpin','unpin','']].forEach(([label,action,kind]) => {
    const button = node('button', label, 'action ' + kind); button.type = 'button';
    button.addEventListener('click', () => taskAction(action)); actions.append(button, document.createTextNode(' '));
  });
  for (const backend of ['codex','claude']) if (backend !== task.preferred_backend) {
    const button = node('button', 'Handoff to ' + backend, 'action'); button.type = 'button';
    button.addEventListener('click', () => taskAction('handoff', backend)); actions.append(button);
  }
  header.append(actions);

  const conversation = byId('conversation');
  (evidence.turns || []).forEach(turn => {
    const item = node('div', null, 'turn'); item.append(node('div', 'You · turn ' + turn.sequence, 'who'), node('pre', turn.user_message));
    if (turn.assistant_message) item.append(node('div', task.preferred_backend, 'who'), node('pre', turn.assistant_message));
    if (turn.error_message) item.append(node('pre', turn.error_code + ': ' + turn.error_message, 'finding'));
    conversation.append(item);
  });
  if (!(evidence.turns || []).length) conversation.append(node('div', 'No turns.', 'empty'));

  const verification = byId('verification');
  (evidence.verification_runs || []).forEach(run => {
    row(verification, run.status + ' · ' + (run.summary || ''), '');
    (run.checks || []).forEach(check => {
      row(verification, '[' + check.status + '] ' + check.name, check.output || check.command);
      (check.findings || []).forEach(finding => row(verification, finding.severity + ' · ' + finding.rule_id, (finding.path || finding.source) + ' · ' + finding.evidence, 'finding'));
    });
  });
  if (!(evidence.verification_runs || []).length) verification.append(node('div', 'No verification yet.', 'empty'));

  const files = byId('files'); (evidence.files_changed || []).forEach(file => row(files, file.operation + ' · ' + file.path, file.previous_path ? 'from ' + file.previous_path : ''));
  if (!(evidence.files_changed || []).length) files.append(node('div', 'No authoritative file changes.', 'empty'));
  const commands = byId('commands'); (evidence.commands || []).forEach(command => row(commands, '[' + command.status + '] ' + command.command, command.aggregated_output));
  if (!(evidence.commands || []).length) commands.append(node('div', 'No commands recorded.', 'empty'));
  const routes = byId('routes'); (evidence.route_attempts || []).forEach(route => row(routes, '#' + route.ordinal + ' · ' + route.backend + ' · ' + route.status, route.decision_reason + (route.failure_class ? ' · ' + route.failure_class : '')));
  const handoffs = byId('handoffs'); (evidence.handoffs || []).forEach(handoff => row(handoffs, handoff.from_backend + ' → ' + handoff.to_backend, short(handoff.handoff_id)));
}

function renderApprovals() {
  const root = clear('approvals');
  const pending = state.approvals.filter(item => !state.selected || item.task_id === state.selected);
  if (!pending.length) return;
  root.className = 'panel'; root.append(node('h3', 'Pending approvals'));
  pending.forEach(approval => {
    const item = node('div', null, 'row'); item.append(node('div', approval.command || approval.grant_root || approval.reason || approval.kind));
    item.append(node('div', approval.backend + ' · ' + short(approval.approval_id), 'empty'));
    for (const decision of ['approve','deny']) {
      const button = node('button', decision, 'action' + (decision === 'deny' ? ' danger' : '')); button.type = 'button';
      button.addEventListener('click', () => decide(approval.approval_id, decision)); item.append(button, document.createTextNode(' '));
    }
    root.append(item);
  });
}

async function refresh() {
  try {
    const [taskResult, backendResult, approvalResult, dashboardContext] = await Promise.all([
      api('/control/v1/tasks?limit=100'), api('/control/v1/backends'), api('/control/v1/approvals?status=pending&limit=100'), api('/control/v1/dashboard-context')
    ]);
    state.workspace = dashboardContext.workspace; state.tasks = (taskResult.tasks || []).filter(task => task.workspace_path === state.workspace);
    state.backends = backendResult.backends || []; state.approvals = approvalResult.approvals || [];
    if (!state.selected && state.tasks.length) state.selected = state.tasks[0].task_id;
    if (state.selected) state.evidence = await api('/control/v1/tasks/' + encodeURIComponent(state.selected) + '/evidence');
    else state.evidence = null;
    renderHealth(); renderTasks(); renderApprovals(); renderEvidence();
  } catch (error) { status('Refresh failed: ' + error.message); }
}

async function selectTask(taskId) { state.selected = taskId; state.evidence = null; renderTasks(); await refresh(); }
async function decide(id, decision) {
  try { await api('/control/v1/approvals/' + encodeURIComponent(id) + '/decision', { method:'POST', body:{ decision } }); status('Decision persisted: ' + decision); await refresh(); }
  catch (error) { status('Decision failed: ' + error.message); }
}
async function taskAction(action, backend) {
  if (!state.selected) return;
  try {
    const path = action === 'handoff' ? '/handoffs' : '/' + action;
    const body = action === 'handoff' ? { to_backend:backend } : undefined;
    await api('/control/v1/tasks/' + encodeURIComponent(state.selected) + path, { method:'POST', body });
    status(action + ' accepted'); await refresh();
  } catch (error) { status(action + ' failed: ' + error.message); }
}

byId('new-form').addEventListener('submit', async event => {
  event.preventDefault(); const prompt = byId('new-prompt').value.trim(); if (!prompt) return;
  const body = { prompt, workspace:state.workspace, permission:byId('new-write').checked ? 'write' : 'read_only' };
  if (byId('new-backend').value) body.backend = byId('new-backend').value;
  try { const task = await api('/control/v1/tasks', { method:'POST', body }); state.selected = task.task_id; byId('new-prompt').value = ''; status('Task started'); await refresh(); }
  catch (error) { status('Start failed: ' + error.message); }
});
byId('continue-form').addEventListener('submit', async event => {
  event.preventDefault(); const prompt = byId('continue-prompt').value.trim(); if (!prompt || !state.selected) return;
  try { await api('/control/v1/tasks/' + encodeURIComponent(state.selected) + '/turns', { method:'POST', body:{ prompt } }); byId('continue-prompt').value = ''; status('Turn started'); await refresh(); }
  catch (error) { status('Continue failed: ' + error.message); }
});

refresh(); setInterval(refresh, 1500);`
