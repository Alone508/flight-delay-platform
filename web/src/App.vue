<template>
  <div class="app-shell">
    <aside class="sidebar">
      <div class="brand">
        <span class="logo">✈</span>
        <div>
          <h1>航班延误平台</h1>
          <p>Flight Delay · Big Data</p>
        </div>
      </div>
      <nav>
        <router-link to="/dashboard">实时监控大屏</router-link>
        <router-link to="/analysis">延误分析</router-link>
        <router-link to="/prediction">预测与模型</router-link>
        <router-link to="/flights">航班明细</router-link>
      </nav>
      <div class="side-foot">
        <span class="dot"></span> 单机伪分布式 · Spark / Kafka / HBase
      </div>
    </aside>
    <main class="main">
      <router-view />
    </main>
  </div>
</template>

<script setup>
</script>

<style>
:root {
  --bg: #070d1b;
  --panel: rgba(17, 28, 51, 0.82);
  --panel-2: rgba(23, 38, 68, 0.72);
  --line: rgba(94, 129, 189, 0.22);
  --text: #e8f0ff;
  --muted: #8ea3c7;
  --cyan: #35e0d0;
  --blue: #4f9cff;
  --amber: #ffb84d;
  --red: #ff5d6c;
}
* { box-sizing: border-box; }
html, body, #app { height: 100%; margin: 0; }
body {
  font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
  color: var(--text);
  background:
    radial-gradient(900px 500px at 80% -10%, rgba(50, 110, 255, 0.22), transparent 60%),
    radial-gradient(700px 500px at 10% 110%, rgba(30, 220, 200, 0.12), transparent 60%),
    var(--bg);
}
.app-shell { display: flex; min-height: 100vh; }
.sidebar {
  width: 232px; flex: 0 0 232px; padding: 22px 16px;
  border-right: 1px solid var(--line);
  background: linear-gradient(180deg, rgba(12, 22, 44, 0.95), rgba(8, 14, 28, 0.9));
  display: flex; flex-direction: column; gap: 26px;
  position: sticky; top: 0; height: 100vh;
}
.brand { display: flex; gap: 10px; align-items: center; }
.logo {
  width: 42px; height: 42px; border-radius: 12px; display: grid; place-items: center;
  background: linear-gradient(135deg, var(--blue), var(--cyan)); color: #04111f;
  font-size: 22px; font-weight: 700; box-shadow: 0 8px 24px rgba(53, 224, 208, 0.25);
}
.brand h1 { font-size: 15px; margin: 0; letter-spacing: 0.5px; }
.brand p { margin: 3px 0 0; color: var(--muted); font-size: 11px; }
nav { display: flex; flex-direction: column; gap: 6px; }
nav a {
  color: var(--muted); text-decoration: none; padding: 11px 12px; border-radius: 10px;
  font-size: 13px; border: 1px solid transparent; transition: all 0.18s ease;
}
nav a:hover { color: var(--text); background: var(--panel-2); }
nav a.router-link-active {
  color: #04111f; font-weight: 700;
  background: linear-gradient(135deg, var(--cyan), #7de6ff);
  box-shadow: 0 8px 22px rgba(53, 224, 208, 0.2);
}
.side-foot { margin-top: auto; color: var(--muted); font-size: 11px; line-height: 1.6; }
.dot { display: inline-block; width: 7px; height: 7px; border-radius: 50%; background: var(--cyan); margin-right: 6px; }
.main { flex: 1; padding: 26px 30px 40px; min-width: 0; }
.page-head { display: flex; align-items: flex-end; justify-content: space-between; margin-bottom: 18px; gap: 16px; }
.page-head h2 { margin: 0; font-size: 22px; letter-spacing: 0.5px; }
.page-head p { margin: 5px 0 0; color: var(--muted); font-size: 12px; }
.grid { display: grid; gap: 16px; }
.cards { grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); }
.card {
  background: var(--panel); border: 1px solid var(--line); border-radius: 16px; padding: 16px;
  backdrop-filter: blur(8px);
}
.card h3 { margin: 0 0 8px; font-size: 12px; color: var(--muted); font-weight: 600; letter-spacing: 0.4px; }
.metric { font-size: 28px; font-weight: 700; font-variant-numeric: tabular-nums; }
.metric.small { font-size: 22px; }
.metric .unit { font-size: 12px; color: var(--muted); font-weight: 500; margin-left: 4px; }
.chart-card { min-height: 320px; }
.chart { width: 100%; height: 280px; }
.chart.tall { height: 360px; }
.table { width: 100%; border-collapse: collapse; font-size: 12px; }
.table th, .table td { padding: 9px 10px; text-align: left; border-bottom: 1px solid var(--line); }
.table th { color: var(--muted); font-weight: 600; }
.table tr:hover td { background: rgba(79, 156, 255, 0.06); }
.tag { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 11px; font-weight: 700; }
.tag.delayed, .tag.WARN { background: rgba(255, 184, 77, 0.15); color: var(--amber); }
.tag.cancelled, .tag.CRITICAL { background: rgba(255, 93, 108, 0.15); color: var(--red); }
.tag.ontime { background: rgba(53, 224, 208, 0.14); color: var(--cyan); }
.empty { color: var(--muted); font-size: 12px; padding: 26px; text-align: center; }
.btn {
  border: 1px solid var(--line); background: var(--panel-2); color: var(--text);
  border-radius: 9px; padding: 8px 12px; font-size: 12px; cursor: pointer;
}
.btn:hover { border-color: var(--cyan); color: var(--cyan); }
.btn.primary { background: linear-gradient(135deg, var(--blue), var(--cyan)); color: #04111f; border: none; font-weight: 700; }
input, select {
  background: rgba(8, 16, 32, 0.8); color: var(--text); border: 1px solid var(--line);
  border-radius: 9px; padding: 8px 10px; font-size: 12px; outline: none;
}
input:focus, select:focus { border-color: var(--cyan); }
.row { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
.muted { color: var(--muted); }
.mono { font-family: "JetBrains Mono", Consolas, monospace; font-size: 11px; }
.modal-mask {
  position: fixed; inset: 0; background: rgba(3, 8, 18, 0.72); display: grid; place-items: center; z-index: 50;
}
.modal {
  width: min(620px, 92vw); max-height: 80vh; overflow: auto; background: #0c1730;
  border: 1px solid var(--line); border-radius: 16px; padding: 20px;
}
.modal h3 { margin-top: 0; }
.kv { display: grid; grid-template-columns: 150px 1fr; gap: 8px 14px; font-size: 12px; }
.kv .k { color: var(--muted); }
@media (max-width: 900px) {
  .app-shell { flex-direction: column; }
  .sidebar { width: 100%; height: auto; position: static; flex-direction: row; flex-wrap: wrap; align-items: center; }
  nav { flex-direction: row; flex-wrap: wrap; }
  .side-foot { display: none; }
  .main { padding: 18px; }
}
</style>
