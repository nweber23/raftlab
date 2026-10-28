"use strict";

// --- constants & helpers -----------------------------------------------------
const SVG = "http://www.w3.org/2000/svg";
const CX = 320, CY = 260, R = 190, NODE_R = 44;
const MIN_FLIGHT_MS = 450; // zero-latency messages would be invisible, so every packet gets at least this
const TERM_COLORS = ["#7fb3ff", "#8fd694", "#f4b942", "#e58fd0", "#79d6d0", "#f08a7c", "#b9a4f5", "#c8d66f"];
const termColor = (t) => TERM_COLORS[t % TERM_COLORS.length];
const $ = (id) => document.getElementById(id);
const el = (name, attrs = {}, parent) => {
  const e = document.createElementNS(SVG, name);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  if (parent) parent.appendChild(e);
  return e;
};

let state = null;
let prev = null;
const pos = {};          // node id -> {x, y}
const nodeEls = {};      // node id -> svg parts
const selected = new Set();
const clientId = "ui-" + Math.random().toString(36).slice(2, 10);
let seq = 0;

// --- websocket ---------------------------------------------------------------
function connect() {
  const ws = new WebSocket((location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws");
  ws.onopen = () => setConn(true);
  ws.onclose = () => { setConn(false); setTimeout(connect, 1000); };
  ws.onmessage = (e) => {
    const m = JSON.parse(e.data);
    if (m.type === "state") onState(m);
    else if (m.type === "msg") onMsg(m);
  };
}
function setConn(on) {
  const c = $("conn");
  c.textContent = on ? "live" : "disconnected — retrying";
  c.className = "pill " + (on ? "on" : "off");
}

// --- stage ------------------------------------------------------------------
function layout(nodes) {
  nodes.forEach((n, i) => {
    const a = -Math.PI / 2 + (2 * Math.PI * i) / nodes.length;
    pos[n.id] = { x: CX + R * Math.cos(a), y: CY + R * Math.sin(a) };
  });
  const links = $("links"), nl = $("nodes");
  for (let i = 0; i < nodes.length; i++) {
    for (let j = i + 1; j < nodes.length; j++) {
      const a = pos[nodes[i].id], b = pos[nodes[j].id];
      el("line", { class: "link", id: `link-${nodes[i].id}-${nodes[j].id}`, x1: a.x, y1: a.y, x2: b.x, y2: b.y }, links);
    }
  }
  for (const n of nodes) {
    const g = el("g", { class: "node follower", transform: `translate(${pos[n.id].x} ${pos[n.id].y})` }, nl);
    el("circle", { class: "body", r: NODE_R }, g);
    const crown = el("text", { class: "crown", y: -NODE_R - 6 }, g); crown.textContent = "♛";
    const id = el("text", { class: "id", y: 7 }, g); id.textContent = "N" + n.id;
    const meta = el("text", { class: "meta", y: NODE_R + 16 }, g);
    const sub = el("text", { class: "meta", y: NODE_R + 30 }, g);
    nodeEls[n.id] = { g, meta, sub };
  }
  const t = $("target");
  for (const n of nodes) t.add(new Option("node " + n.id, n.id));
}

function renderStage(s) {
  for (let i = 0; i < s.nodes.length; i++) {
    for (let j = i + 1; j < s.nodes.length; j++) {
      const l = $(`link-${s.nodes[i].id}-${s.nodes[j].id}`);
      if (l) l.setAttribute("class", "link" + (s.nodes[i].group !== s.nodes[j].group ? " cut" : ""));
    }
  }
  for (const n of s.nodes) {
    const p = nodeEls[n.id];
    const role = n.up ? n.role : "down";
    p.g.setAttribute("class", "node " + role);
    p.meta.textContent = n.up ? `${n.role} · term ${n.term}` : "crashed";
    p.sub.textContent = n.up ? `commit ${n.commit}/${n.lastIndex}` + (n.group ? ` · P${n.group}` : "") : "";
  }
}

// --- messages ---------------------------------------------------------------
const flying = [];
function msgColor(m) {
  if (m.dropped) return "var(--no)";
  switch (m.kind) {
    case "RequestVote": return "var(--vote)";
    case "RequestVoteReply": return m.detail.includes("granted=true") ? "var(--ok)" : "var(--no)";
    case "AppendEntries": return m.detail.includes("entries=0 ") || m.detail.endsWith("entries=0") ? "var(--beat)" : "var(--append)";
    case "AppendEntriesReply": return m.detail.includes("success=true") ? "var(--ok)" : "var(--no)";
    default: return "var(--snap)";
  }
}
function onMsg(m) {
  if (!pos[m.from] || !pos[m.to] || document.hidden) return;
  const c = el("circle", { class: "msg", r: m.kind.startsWith("Append") && m.detail.includes("entries=0") ? 4 : 6, fill: msgColor(m) }, $("msgs"));
  const t = el("title", {}, c);
  t.textContent = `${m.kind} N${m.from}→N${m.to} (term ${m.term}) ${m.detail}` + (m.dropped ? ` — dropped: ${m.reason}` : "") + (m.dup ? " [duplicate]" : "");
  flying.push({ m, c, start: performance.now(), dur: m.delayMs + MIN_FLIGHT_MS });
  if (flying.length > 400) { const old = flying.shift(); old.c.remove(); }
}
function frame(now) {
  for (let i = flying.length - 1; i >= 0; i--) {
    const f = flying[i];
    let k = (now - f.start) / f.dur;
    if (k >= 1) { f.c.remove(); flying.splice(i, 1); continue; }
    const a = pos[f.m.from], b = pos[f.m.to];
    const stop = f.m.dropped ? 0.55 : 1;          // dropped packets die partway
    const kk = Math.min(k / stop, 1);
    f.c.setAttribute("cx", a.x + (b.x - a.x) * kk * stop);
    f.c.setAttribute("cy", a.y + (b.y - a.y) * kk * stop);
    f.c.setAttribute("opacity", f.m.dropped && k > stop ? Math.max(0, 1 - (k - stop) / (1 - stop)) : 1);
  }
  requestAnimationFrame(frame);
}

// --- logs -------------------------------------------------------------------
function cmdLabel(cmd) {
  if (!cmd) return "no-op";
  try {
    const o = JSON.parse(cmd);
    if (o.op === "set") return `${o.key}=${o.value}`;
    if (o.op === "delete") return `del ${o.key}`;
    return `get ${o.key}`;
  } catch { return cmd.slice(0, 8); }
}
const MAX_CELLS = 28;
function renderLogs(s) {
  const root = $("logs");
  if (!root.children.length) {
    for (const n of s.nodes) {
      const row = document.createElement("div");
      row.className = "logrow"; row.id = "log-" + n.id;
      row.innerHTML = `<div class="who"></div><div class="cells"></div><div class="kvtext"></div>`;
      root.appendChild(row);
    }
  }
  for (const n of s.nodes) {
    const row = $("log-" + n.id);
    row.classList.toggle("dead", !n.up);
    row.querySelector(".who").innerHTML = `<b>N${n.id}</b> ${n.up ? n.role : "crashed"}<small>term ${n.term} · applied ${n.applied}</small>`;
    const cells = row.querySelector(".cells");
    cells.textContent = "";
    if (n.snapIndex > 0) cells.appendChild(cell(`snap ≤${n.snapIndex}`, "cell snap", "Compacted into a snapshot: entries up to this index are gone from the log"));
    const log = n.log.length > MAX_CELLS ? n.log.slice(-MAX_CELLS) : n.log;
    if (log.length < n.log.length) cells.appendChild(cell("…", "cell snap", `${n.log.length - log.length} older entries not shown`));
    let marked = n.commit <= n.snapIndex;
    if (marked && n.snapIndex > 0 && n.up) cells.appendChild(marker());
    for (const e of log) {
      const c = cell(`${e.i}·${cmdLabel(e.cmd)}`, "cell" + (e.i > n.commit ? " pending" : ""),
        `index ${e.i}, term ${e.term}${e.i <= n.commit ? ", committed" : ", not committed"}\n${e.cmd || "leader no-op"}`);
      c.style.background = termColor(e.term); c.style.setProperty("--c", termColor(e.term));
      cells.appendChild(c);
      if (!marked && e.i === n.commit) { cells.appendChild(marker()); marked = true; }
    }
    if (!n.log.length && !n.snapIndex && n.up) cells.textContent = "(empty)";
    row.querySelector(".kvtext").textContent = n.up ? "kv " + JSON.stringify(n.kv) : "";
  }
}
function cell(text, cls, title) { const d = document.createElement("span"); d.className = cls; d.textContent = text; d.title = title; return d; }
function marker() { const d = document.createElement("span"); d.className = "commit"; d.title = "commit index"; return d; }

// --- controls ---------------------------------------------------------------
function renderNodeList(s) {
  const ul = $("nodelist");
  if (!ul.children.length) {
    for (const n of s.nodes) {
      const li = document.createElement("li");
      li.id = "nl-" + n.id;
      li.innerHTML = `<input type="checkbox" aria-label="Select node ${n.id}"><span class="name"><b>N${n.id}</b><span class="badge"></span><span class="t"></span></span><button></button>`;
      li.querySelector("input").onchange = (e) => e.target.checked ? selected.add(n.id) : selected.delete(n.id);
      li.querySelector("button").onclick = () => {
        const up = state.nodes.find((x) => x.id === n.id).up;
        post(`/api/${up ? "crash" : "restart"}?id=${n.id}`);
      };
      ul.appendChild(li);
    }
  }
  for (const n of s.nodes) {
    const li = $("nl-" + n.id), role = n.up ? n.role : "down";
    const b = li.querySelector(".badge"); b.textContent = n.up ? n.role : "down"; b.className = "badge " + role;
    li.querySelector(".t").textContent = n.up ? `term ${n.term}` : "";
    const btn = li.querySelector("button");
    btn.textContent = n.up ? "Kill" : "Restart"; btn.className = n.up ? "danger" : "";
  }
}
async function post(url, body) {
  const r = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: body ? JSON.stringify(body) : undefined });
  if (!r.ok) note(`${url}: ${(await r.text()).trim()}`);
}
$("btn-partition").onclick = () => {
  const ids = [...selected];
  if (!ids.length || ids.length === state.nodes.length) return note("select some, but not all, nodes first");
  post("/api/partition", { groups: [ids] });
  note(`partition: ${ids.map((i) => "N" + i).join(", ")} cut off from the rest`);
};
$("btn-isolate-leader").onclick = () => {
  const l = state.nodes.filter((n) => n.up && n.role === "leader").sort((a, b) => b.term - a.term)[0];
  if (!l) return note("no leader right now");
  post("/api/partition", { groups: [[l.id]] });
  note(`partition: leader N${l.id} isolated`);
};
$("btn-heal").onclick = () => { post("/api/heal"); note("partitions healed"); };

let netTimer;
function netInputs() {
  const lat = +$("lat").value, jit = +$("jit").value, drop = +$("drop").value, dup = +$("dup").value;
  $("o-lat").textContent = lat + " ms"; $("o-jit").textContent = jit + " ms";
  $("o-drop").textContent = drop + " %"; $("o-dup").textContent = dup + " %";
  clearTimeout(netTimer);
  netTimer = setTimeout(() => post("/api/network", { minDelayMs: lat, maxDelayMs: lat + jit, dropRate: drop / 100, dupRate: dup / 100 }), 120);
}
for (const id of ["lat", "jit", "drop", "dup"]) $(id).oninput = netInputs;
let netSynced = false;
function syncNet(s) {
  if (netSynced) return; netSynced = true;
  $("lat").value = s.delayMinMs; $("jit").value = s.delayMaxMs - s.delayMinMs;
  $("drop").value = Math.round(s.dropRate * 100); $("dup").value = Math.round(s.dupRate * 100);
  $("o-lat").textContent = $("lat").value + " ms"; $("o-jit").textContent = $("jit").value + " ms";
  $("o-drop").textContent = $("drop").value + " %"; $("o-dup").textContent = $("dup").value + " %";
}

async function kvRequest(op) {
  const key = $("key").value.trim();
  if (!key) return;
  let target = $("target").value;
  if (target === "auto") { // prefer a follower, so the redirect to the leader is visible
    const ups = state.nodes.filter((n) => n.up);
    const followers = ups.filter((n) => n.role !== "leader");
    target = (followers[0] || ups[0] || state.nodes[0]).id;
  }
  const headers = {};
  if (op !== "GET") { headers["X-Client-ID"] = clientId; headers["X-Request-ID"] = String(++seq); }
  const t0 = performance.now();
  try {
    const r = await fetch(`/node/${target}/kv/${encodeURIComponent(key)}`, { method: op, headers, body: op === "PUT" ? $("val").value : undefined });
    const text = (await r.text()).trim();
    const via = r.redirected ? ` (redirected to ${r.url.replace(location.origin, "")})` : "";
    logClient(`${op} ${key} via N${target}${via} → ${r.status}${text ? " " + text : ""}  [${Math.round(performance.now() - t0)} ms]`);
  } catch (e) { logClient(`${op} ${key} via N${target} failed: ${e}`); }
}
$("kvform").onsubmit = (e) => { e.preventDefault(); kvRequest("PUT"); };
for (const b of document.querySelectorAll("#kvform button[data-op]")) b.onclick = (e) => { e.preventDefault(); kvRequest(b.dataset.op); };
function logClient(line) { const p = $("clientlog"); p.textContent = line + "\n" + p.textContent.split("\n").slice(0, 8).join("\n"); }

// --- events -----------------------------------------------------------------
const t0 = Date.now();
function note(text) {
  const li = document.createElement("li");
  const s = ((Date.now() - t0) / 1000).toFixed(1).padStart(6);
  li.innerHTML = `<time>${s}s</time><span></span>`; li.lastChild.textContent = text;
  const ol = $("events"); ol.prepend(li);
  while (ol.children.length > 80) ol.lastChild.remove();
}
function diffEvents(a, b) {
  if (!a) return;
  for (const n of b.nodes) {
    const o = a.nodes.find((x) => x.id === n.id);
    if (!o) continue;
    if (o.up && !n.up) note(`N${n.id} crashed`);
    else if (!o.up && n.up) note(`N${n.id} restarted from its log`);
    else if (n.up && o.role !== n.role) note(`N${n.id} is now ${n.role} (term ${n.term})` + (n.role === "leader" ? " 👑" : ""));
    if (n.up && o.snapIndex !== n.snapIndex && n.snapIndex) note(`N${n.id} compacted its log through index ${n.snapIndex}`);
  }
}

// --- state ------------------------------------------------------------------
function onState(s) {
  if (!state) { layout(s.nodes); syncNet(s); note("connected"); }
  diffEvents(state, s);
  prev = state; state = s;
  renderStage(s); renderNodeList(s); renderLogs(s);
}

connect();
requestAnimationFrame(frame);
