"use strict";

const el = id => document.getElementById(id);
const state = {token:"", role:"", tab:"jobs", jobs:[], workers:[], job:null, worker:null,
  items:[], events:[], audit:[], offset:0, generation:0, busy:false, again:false};
const limit = 50;
const monitor = new LabMonitor();
const statusText = {
  created:"Создано", validating:"Проверка параметров", running:"В работе", pausing:"Переход на паузу",
  paused:"На паузе", stopping:"Останавливается", stopped:"Остановлено", completed:"Завершено",
  failed:"Ошибка", pending:"В ожидании", checking:"Проверяется", not_found:"Отсутствует",
  found:"Найдено", download_queued:"В очереди", downloading:"Скачивается", skipped:"Пропущено",
  forbidden:"Доступ закрыт", rate_limited:"Ограничение частоты", retry_wait:"Ожидает повтора",
  cancelled:"Отменено", online:"На связи", offline:"Не на связи", disabled:"Отключён"
};
const itemStatuses = ["pending","checking","not_found","found","download_queued","downloading","completed","skipped","forbidden","rate_limited","retry_wait","failed","cancelled"];
const num = v => v == null || v === "" || !Number.isFinite(Number(v)) ? "—" : Number(v).toLocaleString("ru-RU",{maximumFractionDigits:2});
const size = v => {
  if (v == null || !Number.isFinite(+v)) return "—";
  let n = +v, unit = 0;
  while (n >= 1024 && unit < 4) { n /= 1024; unit++; }
  return num(n) + " " + ["Б","КиБ","МиБ","ГиБ","ТиБ"][unit];
};
function time(v) {
  if (v == null || v === "") return "—";
  const date = new Date(typeof v === "number" ? (v < 1e12 ? v * 1000 : v) : v);
  return isNaN(date) ? "—" : date.toLocaleString("ru-RU");
}
function duration(v) {
  if (v == null || !Number.isFinite(+v)) return "Не определено";
  const s = Math.max(0, Math.round(v));
  return [Math.floor(s/3600),Math.floor(s%3600/60),s%60].map(n=>String(n).padStart(2,"0")).join(":");
}
function node(tag, text, cls) {
  const result = document.createElement(tag);
  if (text != null) result.textContent = text;
  if (cls) result.className = cls;
  return result;
}
function button(text, fn, cls) {
  const result = node("button", text, cls);
  result.type = "button"; result.addEventListener("click", fn);
  return result;
}
function pill(status) {
  let cls = "";
  if (["online","running","completed","found"].includes(status)) cls = "good";
  if (["failed","forbidden","offline"].includes(status)) cls = "bad";
  if (["paused","pausing","rate_limited","retry_wait"].includes(status)) cls = "warn";
  return node("span", statusText[status] || status || "—", "pill "+cls);
}
function table(id, headers, rows, emptyText) {
  const host = el(id);
  if (!rows.length) {
    host.replaceChildren(node("p", state.token ? emptyText : "Для просмотра данных войдите в систему.", "empty"));
    return;
  }
  const t = node("table"), tr = t.createTHead().insertRow();
  headers.forEach(label => { const th = node("th", label); th.scope="col"; tr.append(th); });
  const body = t.createTBody();
  for (const {cells, selected} of rows) {
    const row = body.insertRow();
    if (selected) row.className = "selected";
    cells.forEach(value => {
      const cell = row.insertCell();
      cell.append(value instanceof Node ? value : document.createTextNode(value ?? "—"));
    });
  }
  host.replaceChildren(t);
}
function facts(id, entries) {
  el(id).replaceChildren(...entries.map(([key,value]) => {
    const group=node("div"); group.append(node("dt",key),node("dd",value??"—")); return group;
  }));
}
function notice(message) { el("notice").textContent=message; el("notice").hidden=!message; }
async function request(path, method="GET", data, token=state.token, csv=false) {
  const response = await fetch("/api"+path, {method,cache:"no-store",credentials:"omit",redirect:"error",
    headers:{Authorization:"Bearer "+token,...(data === undefined ? {} : {"Content-Type":"application/json"})},
    body:data === undefined ? undefined : JSON.stringify(data)});
  if (!response.ok) {
    const body=await response.json().catch(()=>({}));
    throw new Error(body.error?.message || body.message || (typeof body.error==="string" && body.error) || "Ошибка HTTP "+response.status);
  }
  return csv ? response.blob() : (response.status===204 ? {} : await response.json());
}
const jobPath = id => "/jobs/"+encodeURIComponent(id);
const workerPath = id => "/workers/"+encodeURIComponent(id);
const blockLabel = c => c ? "#"+c.id+" · "+c.range_start+"…"+c.range_end : "—";
const operate = () => ["operator","administrator"].includes(state.role);
const admin = () => state.role === "administrator";
const query = () => {
  const params = new URLSearchParams();
  if (el("filter-id").value.trim()) params.set("external_id",el("filter-id").value.trim());
  if (el("filter-status").value) params.set("status",el("filter-status").value);
  return params;
};
function selectTab(tab) {
  state.tab=tab; state.generation++;
  for (const n of document.querySelectorAll("[data-tab]")) {
    const active=n.dataset.tab===tab;
    n.toggleAttribute("aria-current",active);
    if (active) n.setAttribute("aria-current","page");
    el(n.dataset.tab+"-view").hidden=!active;
  }
  refresh();
}
function renderOverview() {
  const {jobs,workers}=state;
  el("new-job").disabled=!operate(); el("audit-tab").hidden=!admin();
  el("running").textContent=state.token ? num(jobs.filter(j=>j.status==="running").length) : "—";
  el("online").textContent=state.token ? num(workers.filter(w=>w.online).length) : "—";
  el("node-total").textContent=state.token ? "из "+num(workers.length)+" серверов" : "серверов";
  for (const [id,key] of [["processed","processed"],["files","downloaded"]]) {
    el(id).textContent=state.token ? num(jobs.reduce((v,j)=>v+(Number(j[key])||0),0)) : "—";
  }
  table("jobs-list",["Задание","Состояние","Диапазон ID","Обработано","Скорость, ID/с","Ошибки"],
    jobs.map(j=>({selected:j.id===state.job,cells:[
      button(j.name||"Задание "+j.id,()=>{state.job=j.id;state.offset=0;state.generation++;refresh();},"link"),
      pill(j.status),num(j.range_start)+" … "+(j.range_end==null?"∞":num(j.range_end)),num(j.processed),num(j.speed),num(j.errors)]})),
    "Заданий пока нет.");
  table("workers-list",["Узел","Подключение","Потоки","Блок","Активные загрузки","Последняя связь"],
    workers.map(w=>({selected:w.id===state.worker,cells:[
      button(w.name||w.id,()=>{state.worker=w.id;state.generation++;refresh();},"link"),
      pill(!w.enabled?"disabled":w.online?"online":"offline"),num(w.thread_override??w.threads),blockLabel(w.current_chunk),num(w.metrics?.active_downloads),time(w.last_heartbeat)]})),
    "Нет зарегистрированных узлов.");
  const alerts=monitor.evaluate(jobs,workers,Date.now()/1000);
  el("warnings").hidden=!alerts.length;
  el("warning-count").textContent="Предупреждения · "+alerts.length;
  el("warning-list").replaceChildren(...alerts.map(a=>node("li",a.message)));
}
function renderJob(j) {
  el("job-detail").hidden=!j;
  if (!j) return;
  el("job-title").textContent=j.name||"Задание "+j.id;
  el("job-state").textContent=(statusText[j.status]||j.status)+" · "+j.id;
  const count="total_items" in j ? j.total_items : (j.range_end==null?null:j.range_end-j.range_start+1);
  const active=state.workers.filter(w=>w.online && w.current_chunk?.job_id===j.id);
  const sum=k=>active.reduce((total,w)=>total+(Number(w.metrics?.[k])||0),0);
  facts("job-facts",[
    ["Прогресс",count==null?num(j.processed)+" ID":num(j.processed)+" / "+num(count)+" ("+num(100*j.processed/count)+" %)"],
    ["Найдено",num(j.found)],["Загружено файлов",num(j.downloaded)],["Ошибки",num(j.errors)],
    ["Скачано данных",size(j.bytes_downloaded)],["Средняя обработка",num(j.speed)+" ID/с"],
    ["Текущая обработка",num(sum("processed_per_second"))+" ID/с"],["Средняя загрузка",size(j.download_speed)+"/с"],
    ["Текущая загрузка",size(sum("download_bytes_per_second"))+"/с"],["Активные серверы",num(active.length)],
    ["Занятые потоки",num(sum("active_checks")+sum("active_downloads"))],["Активные загрузки",num(sum("active_downloads"))],
    ["Оставшееся время",count==null?"Не определено":duration(j.eta_seconds)],
    ["Потоков на узел",num(j.threads)],["Общий лимит, запросов/с",num(j.rate_limit)],
    ["Лимит проверок, запросов/с",j.check_rps==null?"Общий бюджет":num(j.check_rps)],
    ["Лимит загрузок, запросов/с",j.download_rps==null?"Общий бюджет":num(j.download_rps)],
    ["Размер блока",num(j.chunk_size)],["Блоков в очереди",num(j.queue_size)],
    ["Старейший блок в очереди",j.queue_size?duration(j.oldest_queued_seconds):"Очередь пуста"],
    ["Последний прогресс",time(j.last_progress_at)],["Создано",time(j.created_at)]
  ]);
  const actions=el("job-actions"); actions.replaceChildren();
  if (!operate()) return;
  for (const [action,label,states] of [
    ["start","Запустить",["created"]],["pause","Мягкая пауза",["running"]],["resume","Продолжить",["paused"]],
    ["stop","Остановить",["created","running","paused","pausing"]],["retry","Повторить ошибки",["stopped","paused","completed","failed"]]
  ]) {
    if (!states.includes(j.status) || (action==="retry" && !j.errors)) continue;
    actions.append(button(label,()=>{
      const exec=()=>request(jobPath(j.id)+"/"+action,"POST",{});
      if (action==="stop") confirmAction(label,"Активные операции будут остановлены с сохранением прогресса.",exec);
      else run(exec);
    },action==="stop"?"danger":""));
  }
  actions.append(button("Настройки",()=>settings("Параметры задания",jobPath(j.id),[
    ["threads","Потоков на узел",j.threads,1],["rate_limit","Общий лимит, запросов/с",j.rate_limit,1],
    ["check_rps","Проверки, запросов/с",j.check_rps,1,true],["download_rps","Загрузки, запросов/с",j.download_rps,1,true]
  ],j.worker_ids||[])));
}
function renderItems() {
  const states=el("items-mode").value==="states";
  const headers=states?["ID","Статус","Сервер","Блок","Попытка","Время"]:["ID","Статус","Материал","Сервер","Размер","Диагностика"];
  table("items-list",headers,state.items.map(i=>({cells:[
    button(String(i.external_id),()=>showHistory(state.job,i.external_id),"link"),pill(i.status),...(states?
      [i.worker_id,i.chunk_id,i.attempt,time(i.ts)]:[i.metadata?.title,i.worker_id,size(i.file?.size??i.file?.size_bytes??i.metadata?.size),i.error||"—"])]})),
    "Записей с такими параметрами пока нет.");
  el("export").hidden=states;
  el("page").textContent="Страница "+(1+Math.floor(state.offset/limit));
  el("prev").disabled=state.offset===0; el("next").disabled=state.items.length<limit;
}
let historyRequest=0;
async function showHistory(job,id,offset=0) {
  const dialog=el("history-dialog"), ticket=++historyRequest;
  if (!dialog.open) dialog.showModal();
  el("history-title").textContent="История ID "+id;
  el("history-list").textContent="Загрузка…";
  el("history-prev").disabled=true;el("history-next").disabled=true;
  try {
    const rows=await request(jobPath(job)+"/items/"+encodeURIComponent(id)+"/history?limit="+limit+"&offset="+offset);
    if (!dialog.open || ticket!==historyRequest) return;
    table("history-list",["Время","Статус","Узел","Попытка"],rows.map(i=>({cells:[time(i.ts),pill(i.status),i.worker_id,i.attempt]})),"История пока пуста.");
    el("history-prev").disabled=offset===0;el("history-next").disabled=rows.length<limit;
    el("history-prev").onclick=()=>showHistory(job,id,offset-limit);
    el("history-next").onclick=()=>showHistory(job,id,offset+limit);
  } catch(err) { if (ticket===historyRequest) el("history-list").textContent=err.message; }
}
function renderEvents(id,source,term) {
  const rows=source.filter(e=>Object.values(e).join(" ").toLowerCase().includes(term.toLowerCase()));
  table(id,["Время","Компонент / пользователь","Операция","Результат","Узел / ID","Сообщение"],
    rows.map(e=>({cells:[time(e.ts),e.component||e.actor||e.role,e.operation||e.action,e.result||e.status,
      [e.worker_id,e.external_id].filter(v=>v!=null).join(" · ")||"—",e.message||e.detail||"—"]})),
    "Нет подходящих записей.");
}
const metricLabels = {
  active_checks:"Активные проверки",active_downloads:"Активные загрузки",free_space_gb:"Свободно, ГБ",
  total_space_gb:"Объём диска, ГБ",uptime_seconds:"Время работы, с",processed_per_second:"Скорость проверки, ID/с",
  download_bytes_per_second:"Скорость загрузки, Б/с",bytes_downloaded:"Загружено, Б",processed:"Обработано ID",
  found:"Найдено",errors:"Ошибки",retries:"Повторы",http_requests:"HTTP-запросы",http_statuses:"HTTP-статусы",
  cpu_percent:"Загрузка CPU, %",load_average:"Системная нагрузка",ram_total_bytes:"RAM всего, Б",ram_used_bytes:"RAM занято, Б",
  swap_used_bytes:"Swap занято, Б",disk_read_bytes:"Прочитано с диска, Б",disk_write_bytes:"Записано на диск, Б",
  network_rx_bytes:"Входящий трафик, Б",network_tx_bytes:"Исходящий трафик, Б",open_files:"Открытые файлы",
  active_connections:"Активные соединения",active_items:"Активные материалы",avg_check_seconds:"Средняя проверка, с",avg_download_seconds:"Средняя загрузка, с"
};
function renderWorker(w) {
  el("worker-detail").hidden=!w;
  renderService(w);
  if (!w) return;
  el("worker-title").textContent=w.name||w.id;
  facts("worker-facts",[["ID",w.id],["Адрес",w.host||w.address],["Версия агента",w.version],
    ["Последняя связь",time(w.last_heartbeat)],["Текущий блок",blockLabel(w.current_chunk)],["Потоки",num(w.thread_override??w.threads)]]);
  const metrics=w.metrics||{};
  const display=Object.entries(metrics).map(([key,value])=>{
    let valueText=value;
    if (key==="active_items" && Array.isArray(value)) {
      valueText=value.map(i=>[i.external_id,i.title,statusText[i.status]||i.status,size(i.bytes)+" / "+size(i.size)].filter(Boolean).join(" · ")).join("\n")||"—";
    } else if (typeof value==="number") valueText=num(value);
    else if (typeof value==="object") valueText=JSON.stringify(value);
    return [metricLabels[key]||key,valueText];
  });
  facts("worker-metrics",display.length?display:[["Состояние","Ожидание первого отчёта агента"]]);
  const host=el("worker-actions");host.replaceChildren();
  if (!operate()) return;
  host.append(button("Настройки узла",()=>settings("Параметры узла",workerPath(w.id),[
    ["threads","Потоки",w.thread_override,1,true],["worker_rps","Общий лимит узла, запросов/с",w.worker_rps,1,true],
    ["check_rps","Проверки, запросов/с",w.check_rps,1,true],["download_rps","Загрузки, запросов/с",w.download_rps,1,true]
  ])));
  host.append(button(w.enabled?"Отключить":"Включить",()=>{
    const exec=()=>request(workerPath(w.id),"PATCH",{enabled:!w.enabled});
    if (w.enabled) confirmAction("Отключить узел","Координатор прекратит выдавать узлу новые блоки.",exec);
    else run(exec);
  }));
}
const serviceInfo=new Map();
const serviceActions=[
  ["status","Проверить службу",""],
  ["start","Запустить службу","Координатор отправит команду запуска службы агента."],
  ["restart","Перезапустить службу","Координатор перезапустит службу агента. Текущие операции могут прерваться."],
  ["update","Обновить агент","Координатор запустит установленную на узле службу обновления агента."]
];
function renderService(w) {
  const panel=el("service-panel"), actions=el("service-actions"), feedback=el("service-result");
  panel.hidden=!w||!admin(); actions.replaceChildren(); feedback.textContent="";
  if (panel.hidden) return;
  const configured=w.ssh_configured===true;
  const info=serviceInfo.get(w.id);
  feedback.textContent=configured ? info?.message||"" : "Узел отсутствует в SSH-инвентаре координатора.";
  for (const [action,label,explanation] of serviceActions) {
    const invoke=async()=>{
      if (serviceInfo.get(w.id)?.pending) return;
      const token=state.token;
      serviceInfo.set(w.id,{pending:true,message:label+"…"});
      if (state.worker===w.id) renderService(w);
      try {
        const response=await request(workerPath(w.id)+"/service","POST",{action});
        const message=action==="status" ? (response.status||"Служба активна.") : "Выполнено: "+label+".";
        if (state.token===token) serviceInfo.set(w.id,{message});
      } catch (err) {
        if (state.token===token) serviceInfo.set(w.id,{message:"Операция не выполнена. "+err.message});
        throw err;
      } finally {
        if (state.worker===w.id && state.token===token) renderService(w);
      }
    };
    const control=button(label,()=>{
      if (action==="status") run(invoke);
      else confirmAction(label+" · "+(w.name||w.id),explanation,invoke);
    });
    control.disabled=!configured||Boolean(info?.pending);
    actions.append(control);
  }
}
async function refresh() {
  if (!state.token) return;
  if (state.busy) { state.again=true; return; }
  state.busy=true;
  const generation=state.generation;
  try {
    const [jobs,workers]=await Promise.all([request("/jobs"),request("/workers")]);
    if (generation!==state.generation) return;
    state.jobs=jobs;state.workers=workers;
    if (!jobs.some(j=>j.id===state.job)) {state.job=jobs[0]?.id??null;state.offset=0;}
    if (!workers.some(w=>w.id===state.worker)) state.worker=workers[0]?.id??null;
    renderOverview();
    if (state.tab==="jobs") {
      const j=jobs.find(j=>j.id===state.job);
      renderJob(j);
      if (j) {
        const params=query(); params.set("limit",limit); params.set("offset",state.offset);
        const [items,events]=await Promise.all([request(jobPath(j.id)+"/"+el("items-mode").value+"?"+params),request(jobPath(j.id)+"/events")]);
        if (generation!==state.generation) return;
        state.items=items;state.events=events;renderItems();renderEvents("events-list",events,el("filter-event").value);
      }
    } else if (state.tab==="workers") renderWorker(workers.find(w=>w.id===state.worker));
    else if (state.tab==="audit" && admin()) {
      const audit=await request("/audit");
      if (generation!==state.generation) return;
      state.audit=audit; renderEvents("audit-list",audit,el("audit-search").value);
    }
    el("updated").textContent="Обновлено "+new Date().toLocaleTimeString("ru-RU");
    el("connection").textContent=({viewer:"Наблюдатель",operator:"Оператор",administrator:"Администратор"})[state.role]||"На связи";
  } catch (err) {
    if (generation===state.generation) {notice(err.message);el("connection").textContent="Ошибка обновления";}
  } finally {
    state.busy=false;
    if (state.again) {state.again=false;queueMicrotask(refresh);}
  }
}
function logout() {
  state.generation++;
  serviceInfo.clear();
  Object.assign(state,{token:"",role:"",jobs:[],workers:[],items:[],events:[],audit:[],job:null,worker:null,offset:0});
  el("connection").textContent="Нет подключения";el("session-button").textContent="Войти";
  el("updated").textContent="Данные координатора";
  el("auth-form").reset();el("job-form").reset();el("filter-id").value="";el("filter-status").value="";
  el("events-list").replaceChildren();el("items-list").replaceChildren();el("audit-list").replaceChildren();
  el("job-facts").replaceChildren();el("worker-facts").replaceChildren();el("worker-metrics").replaceChildren();
  renderOverview();renderJob(null);renderWorker(null);notice("");selectTab("jobs");
}
function openDialog(id) {
  const dlg=el(id);dlg.querySelector(".form-error").textContent="";dlg.showModal();
}
function formAction(formId, fn) {
  const form=el(formId);
  form.onsubmit=async event=>{
    event.preventDefault();
    const submit=form.querySelector("button:not([type=button])");submit.disabled=true;
    try { await fn(new FormData(form)); form.closest("dialog").close();notice("");await refresh(); }
    catch(err) { form.querySelector(".form-error").textContent=err.message; }
    finally { submit.disabled=false; }
  };
}
async function run(fn) { try { await fn();notice("");await refresh(); } catch(err) { notice(err.message); } }
function settings(title,path,specs,workerIds) {
  el("settings-title").textContent=title;
  el("settings-fields").replaceChildren(...specs.map(([name,title,value,min,optional])=>{
    const label=node("label",title),input=node("input");
    Object.assign(input,{name,type:"number",min,value:value??(optional?"":min),required:!optional,
      placeholder:optional?"По умолчанию":""});label.append(input);return label;
  }));
  if (workerIds) {
    const label=node("label","Рабочие серверы (без выбора — все)"),select=node("select");
    select.multiple=true;select.name="worker_ids"; label.append(select);
    state.workers.forEach(w=>select.add(new Option(w.name||w.id,w.id,false,workerIds.includes(w.id))));
    el("settings-fields").append(label);
  }
  formAction("settings-form",form=>{
    const data=Object.fromEntries(specs.map(([key])=>[key,form.get(key)===""?null:Number(form.get(key))]));
    if (workerIds) data.worker_ids=form.getAll("worker_ids");
    return request(path,"PATCH",data);
  });
  openDialog("settings-dialog");
}
function confirmAction(title, text, fn) {
  el("confirm-title").textContent=title;el("confirm-text").textContent=text;
  formAction("confirm-form",fn);openDialog("confirm-dialog");
}
formAction("auth-form",async form=>{
  const token=form.get("token").trim();
  const me=await request("/me","GET",undefined,token);
  state.generation++;state.token=token;state.role=me.role;
  el("auth-form").reset();el("session-button").textContent="Выйти";
});
formAction("job-form",async form=>{
  const data=Object.fromEntries(form);
  if (!data.name.trim()) delete data.name;
  for (const key of ["max_items","max_seconds","stop_on_consecutive_missing","check_rps","download_rps"]) data[key]=data[key]===""?null:Number(data[key]);
  for (const key of ["threads","rate_limit","chunk_size","max_retries","request_timeout_seconds","min_free_space_gb"]) data[key]=Number(data[key]);
  data.worker_ids=form.getAll("worker_ids");if (!data.worker_ids.length) delete data.worker_ids;
  const created=await request("/jobs","POST",data);
  state.job=created.id;state.offset=0;selectTab("jobs");
});
el("session-button").onclick=()=>state.token?logout():openDialog("auth-dialog");
el("new-job").onclick=()=>{
  const select=el("job-form").elements.worker_ids;
  select.replaceChildren(...state.workers.map(w=>new Option(w.name||w.id,w.id)));
  openDialog("job-dialog");
};
for (const b of document.querySelectorAll("[data-close]")) b.onclick=()=>b.closest("dialog").close();
for (const b of document.querySelectorAll("[data-tab]")) b.onclick=()=>selectTab(b.dataset.tab);
for (const s of itemStatuses) el("filter-status").add(new Option(statusText[s],s));
el("refresh").onclick=()=>{notice("");refresh();};
function page(offset) {state.offset=offset;state.generation++;refresh();}
let delay;
el("filter-id").oninput=()=>{clearTimeout(delay);delay=setTimeout(()=>page(0),250);};
el("filter-status").onchange=()=>page(0);
el("items-mode").onchange=()=>page(0);
el("filter-event").oninput=()=>renderEvents("events-list",state.events,el("filter-event").value);
el("audit-search").oninput=()=>renderEvents("audit-list",state.audit,el("audit-search").value);
el("prev").onclick=()=>page(Math.max(0,state.offset-limit)); el("next").onclick=()=>page(state.offset+limit);
el("export").onclick=()=>run(async()=>{
  const params=query();params.set("format","csv");
  const blob=await request(jobPath(state.job)+"/items?"+params,"GET",undefined,state.token,true);
  const url=URL.createObjectURL(blob),a=node("a");
  a.href=url;a.download="job-"+state.job+"-results.csv";a.click();setTimeout(()=>URL.revokeObjectURL(url),30000);
});
logout();
setInterval(()=>refresh(),3000);
