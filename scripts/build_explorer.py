#!/usr/bin/env python3
"""Bangun ui/project_explorer.html: daftar file project + isinya, satu file HTML mandiri.
Jalankan ulang setiap kali file project berubah:  python scripts/build_explorer.py
File data besar (csv/sql) hanya ditampilkan metadatanya, bukan isinya."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "ui" / "project_explorer.html"
SKIP_DIRS = {".git", "__pycache__", ".venv", "node_modules", ".ipynb_checkpoints"}
SKIP_FILES = {"ui/project_explorer.html"}
DATA_HIDDEN = {".csv", ".sql"}

DESC = {
 "README.md": "Panduan setup, alur pipeline, endpoint API, dan daftar GitHub secrets.",
 ".env.example": "Template konfigurasi server. Salin menjadi .env, jangan di-commit.",
 ".gitignore": "Daftar file yang tidak ikut git.",
 "requirements.txt": "Dependensi notebook dan API (dipasang di .venv server).",
 ".github/workflows/mlops-pipeline.yml": "CI/CD: SSH ke server, trigger DAG, jalankan notebook, reload API.",
 "dags/oracle_to_clickhouse_ai4i.py": "DAG Airflow: Oracle 26ai ke ClickHouse, full refresh dengan validasi.",
 "dags/requirements-airflow.txt": "Dependensi untuk environment Airflow.",
 "data/ai4i2020.csv": "Data sumber AI4I 2020. Isi tidak ditampilkan di halaman ini.",
 "ecosystem.config.js": "Konfigurasi pm2 untuk menjalankan uvicorn.",
 "notebooks/SMOTE_ai4i2020_xgboost_machine_failure.ipynb": "Training baseline + SMOTE, log ke MLflow, daftar ke Registry. Ditampilkan sebagai teks per sel.",
 "runs/.gitkeep": "Penanda agar folder hasil eksekusi notebook ikut tersimpan di git.",
 "scripts/build_explorer.py": "Membangun halaman penjelajah file ini.",
 "scripts/csv_to_oracle_insert.py": "Convert CSV menjadi script INSERT Oracle 26ai.",
 "scripts/deploy_remote.sh": "Dijalankan di server oleh workflow: update, DAG, notebook, reload.",
 "scripts/trigger_airflow_dag.py": "Trigger DAG lewat REST API dan tunggu sampai selesai.",
 "serving/app.py": "FastAPI. Model dimuat dari MLflow Model Registry, bukan file.",
 "sql/insert_ai4i2020.sql": "Hasil generate INSERT untuk Oracle. Isi tidak ditampilkan di halaman ini.",
 "ui/dummy_dashboard.html": "Contoh tampilan form prediksi.",
 "ui/pipeline_monitor.html": "Contoh tampilan status pipeline dan model registry.",
}

def notebook_text(path: Path) -> str:
    nb = json.loads(path.read_text(encoding="utf-8"))
    parts = []
    for i, c in enumerate(nb["cells"], 1):
        body = "".join(c["source"]).rstrip("\n")
        if c["cell_type"] == "markdown":
            parts.append(f"[sel {i} markdown]\n" + "\n".join("# " + l for l in body.split("\n")))
        else:
            parts.append(f"[sel {i} kode]\n{body}")
    return "\n\n".join(parts) + "\n"

files = []
for p in sorted(ROOT.rglob("*")):
    rel = p.relative_to(ROOT).as_posix()
    if not p.is_file() or rel in SKIP_FILES or p.suffix == ".zip":
        continue
    if any(part in SKIP_DIRS for part in p.relative_to(ROOT).parts):
        continue
    if rel.startswith("runs/executed_"):
        continue
    size = p.stat().st_size
    if p.suffix in DATA_HIDDEN:
        lines = sum(1 for _ in p.open("rb"))
        files.append(dict(path=rel, size=size, lines=lines, kind="data", desc=DESC.get(rel, ""), content=""))
        continue
    text = notebook_text(p) if p.suffix == ".ipynb" else p.read_text(encoding="utf-8")
    files.append(dict(path=rel, size=size, lines=len(text.splitlines()), kind="text" if text else "empty",
                      desc=DESC.get(rel, ""), content=text))

TEMPLATE = r'''<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Penjelajah file project (dummy)</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Archivo:wght@400;600;800&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
<style>
:root{--bg:#eef1ee;--panel:#fff;--ink:#16262b;--muted:#5b6b70;--line:#cbd4d1;--sel:#dde7e3;--code:#f6f8f6;--accent:#1f7a5a;
  --sans:"Archivo",system-ui,-apple-system,"Segoe UI",sans-serif;--mono:"JetBrains Mono",ui-monospace,Menlo,Consolas,monospace;color-scheme:light}
@media (prefers-color-scheme:dark){:root{--bg:#111a1d;--panel:#182327;--ink:#e6eeeb;--muted:#9aaba9;--line:#2b3a3e;--sel:#233236;--code:#131d20;--accent:#4dbf96;color-scheme:dark}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:400 15px/1.5 var(--sans)}
.wrap{max-width:1240px;margin:0 auto;padding:22px 20px 40px}
header{display:flex;flex-wrap:wrap;justify-content:space-between;align-items:flex-end;gap:10px;border-bottom:2px solid var(--ink);padding-bottom:12px}
h1{margin:0;font-size:24px;font-weight:800;letter-spacing:-.01em}
.stats{font:400 12px var(--mono);color:var(--muted)}
.layout{display:grid;grid-template-columns:320px 1fr;gap:18px;margin-top:18px;align-items:start}
@media (max-width:860px){.layout{grid-template-columns:1fr}}
.side,.view{background:var(--panel);border:1px solid var(--line);border-radius:6px}
.side{padding:12px;max-height:78vh;overflow:auto}
#q{width:100%;padding:8px 10px;margin-bottom:10px;font:400 14px var(--mono);color:var(--ink);background:var(--bg);border:1px solid var(--line);border-radius:4px}
#q:focus-visible,.file:focus-visible,summary:focus-visible,#copy:focus-visible{outline:2px solid var(--ink);outline-offset:1px}
details{margin-left:2px} details>div,details>.kids{margin-left:14px;border-left:1px solid var(--line);padding-left:6px}
summary{cursor:pointer;padding:3px 4px;font-weight:600;font-size:14px;list-style-position:inside}
.file{display:block;width:100%;text-align:left;background:none;border:0;border-radius:3px;padding:3px 6px;cursor:pointer;font:400 13px var(--mono);color:var(--ink)}
.file:hover{background:var(--sel)} .file[aria-current="true"]{background:var(--sel);box-shadow:inset 3px 0 var(--accent);font-weight:600}
.file[hidden],details[hidden]{display:none}
.view{min-width:0}
.vhead{padding:14px 18px;border-bottom:1px solid var(--line)}
.vhead h2{margin:0;font:600 15px var(--mono);word-break:break-all}
.vhead p{margin:4px 0 0;color:var(--muted);font-size:14px}
.meta{display:flex;flex-wrap:wrap;gap:14px;align-items:center;margin-top:8px;font:400 12px var(--mono);color:var(--muted)}
#copy{font:600 12px var(--sans);padding:4px 10px;border:1px solid var(--line);border-radius:4px;background:transparent;color:var(--ink);cursor:pointer}
#copy:hover{border-color:var(--ink)}
pre{margin:0;padding:14px 0;background:var(--code);overflow:auto;max-height:70vh;font:400 12.5px/1.55 var(--mono);counter-reset:l;border-radius:0 0 6px 6px}
.l{display:block;padding:0 18px 0 0;white-space:pre}
.l::before{counter-increment:l;content:counter(l);display:inline-block;width:3.6em;padding-right:1em;text-align:right;color:var(--muted);user-select:none}
.empty{padding:28px 18px;color:var(--muted)}
.note{margin:14px 2px 0;font-size:12px;color:var(--muted)}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>machine-failure-mlops</h1>
    <div class="stats" id="stats"></div>
  </header>
  <div class="layout">
    <nav class="side" aria-label="Daftar file">
      <input id="q" type="search" placeholder="Cari nama file" aria-label="Cari nama file">
      <div id="tree"></div>
    </nav>
    <main class="view" id="view" aria-live="polite"></main>
  </div>
  <p class="note">Tampilan contoh. Daftar dan isi file di halaman ini sudah tertanam saat dibangun (scripts/build_explorer.py), tidak membaca server atau repo.</p>
</div>
<script>
const FILES = /*__DATA__*/[];
const $ = id => document.getElementById(id);
const esc = s => s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
const kb = n => n < 1024 ? n + " B" : (n/1024).toFixed(1).replace(".", ",") + " KB";

const root = {};
FILES.forEach((f, i) => {
  let n = root; const parts = f.path.split("/");
  parts.forEach((p, k) => { if (k === parts.length-1) (n.__f = n.__f || []).push({name:p, i}); else n = (n[p] = n[p] || {}); });
});
function render(node){
  const dirs = Object.keys(node).filter(k => k !== "__f").sort();
  let h = dirs.map(d => `<details open><summary>${esc(d)}/</summary><div>${render(node[d])}</div></details>`).join("");
  h += (node.__f || []).sort((a,b) => a.name.localeCompare(b.name))
        .map(f => `<button class="file" data-i="${f.i}" data-n="${esc(FILES[f.i].path.toLowerCase())}">${esc(f.name)}</button>`).join("");
  return h;
}
$("tree").innerHTML = render(root);

const totalLines = FILES.filter(f => f.kind === "text").reduce((a,f) => a + f.lines, 0);
$("stats").textContent = FILES.length + " file, " + new Intl.NumberFormat("id-ID").format(totalLines) + " baris kode dan teks";

let current = null;
function show(i){
  const f = FILES[i]; current = f;
  document.querySelectorAll(".file").forEach(b => b.setAttribute("aria-current", b.dataset.i == i ? "true" : "false"));
  let body;
  if (f.kind === "data") body = `<div class="empty">File data (${new Intl.NumberFormat("id-ID").format(f.lines)} baris). Isinya tidak ditampilkan di halaman ini.</div>`;
  else if (f.kind === "empty") body = `<div class="empty">File kosong.</div>`;
  else body = "<pre><code>" + f.content.replace(/\n$/, "").split("\n").map(l => `<span class="l">${esc(l) || " "}</span>`).join("") + "</code></pre>";
  $("view").innerHTML = `<div class="vhead"><h2>${esc(f.path)}</h2><p>${esc(f.desc)}</p>
    <div class="meta"><span>${kb(f.size)}</span><span>${f.lines} baris</span>${f.kind === "text" ? '<button id="copy" type="button">Salin isi</button>' : ""}</div></div>${body}`;
  const c = $("copy");
  if (c) c.onclick = async () => { try { await navigator.clipboard.writeText(current.content); c.textContent = "Tersalin"; }
                                   catch(e) { c.textContent = "Tidak bisa menyalin"; } setTimeout(() => c.textContent = "Salin isi", 1500); };
}
document.querySelectorAll(".file").forEach(b => b.onclick = () => show(+b.dataset.i));

$("q").oninput = e => {
  const q = e.target.value.trim().toLowerCase();
  document.querySelectorAll(".file").forEach(b => b.hidden = q && !b.dataset.n.includes(q));
  document.querySelectorAll("details").forEach(d => d.hidden = q && !d.querySelector(".file:not([hidden])"));
};
const start = FILES.findIndex(f => f.path === "README.md");
show(start >= 0 ? start : 0);
</script>
</body>
</html>
'''

data = json.dumps(files, ensure_ascii=False).replace("<", "\\u003c")  # aman di dalam <script>
OUT.write_text(TEMPLATE.replace("/*__DATA__*/[]", data), encoding="utf-8")
print(f"OK: {len(files)} file -> {OUT.relative_to(ROOT)} ({OUT.stat().st_size // 1024} KB)")
