"use strict";

// Keep counter samples only for the current tab. Duplicate heartbeats add no samples.
class LabMonitor {
  constructor() { this.series = new Map(); }

  intervals(key, timestamp, counts, period=30, uptime=null) {
    let history=this.series.get(key)||[];
    const last=history.at(-1);
    if (last && (timestamp<last.ts || timestamp-last.ts>period*2 ||
        (uptime!=null && last.uptime!=null && uptime<last.uptime) ||
        Object.keys(counts).some(k=>counts[k]<last.counts[k]))) history=[];
    if (timestamp!==history.at(-1)?.ts) history.push({ts:timestamp,counts,uptime});
    history=history.filter(s=>timestamp-s.ts<=period*4);
    this.series.set(key,history);
    const end=history.at(-1);
    const before=(boundary)=>history.findLast(s=>s.ts<=boundary);
    const middle=before(end.ts-period);
    if (!middle || end.ts-middle.ts>period*2) return {};
    const start=before(middle.ts-period);
    const delta=(a,b)=>{
      const result={seconds:b.ts-a.ts};
      for (const k of Object.keys(counts)) result[k]=b.counts[k]-a.counts[k];
      return result;
    };
    return {recent:delta(middle,end), prior:start && middle.ts-start.ts<=period*2 ? delta(start,middle) : null};
  }

  evaluate(jobs,workers,now) {
    const out=[],live=new Set();
    const alert=(kind,owner,text)=>out.push({kind,message:owner+" — "+text});
    for (const w of workers.filter(w=>w.enabled)) {
      const label=w.name||w.id, m=w.metrics||{};
      if (w.expected_agent_version && w.version!==w.expected_agent_version)
        alert("version",label,"версия агента "+w.version+", ожидается "+w.expected_agent_version);
      if (!w.online) {
        alert("offline",label,"координатор считает сервер недоступным для работы");
        continue;
      }
      if (w.last_heartbeat!=null && now-w.last_heartbeat>1.5*w.heartbeat_interval_seconds)
        alert("heartbeat",label,"heartbeat задержан");
      if (m.cpu_percent>=90 || (m.ram_total_bytes>0 && m.ram_used_bytes/m.ram_total_bytes>=0.9))
        alert("resources",label,"загрузка CPU или RAM достигла 90 %");
      const affected=jobs.filter(j=>j.status==="running" && (!j.worker_ids?.length || j.worker_ids.includes(w.id)) &&
        m.free_space_gb!=null && m.free_space_gb<j.min_free_space_gb);
      if (affected.length) alert("disk",label,"свободное место ниже резерва заданий "+affected.map(j=>j.id).join(", "));
      if (!Number.isFinite(m.http_requests) || !Number.isFinite(w.last_heartbeat)) continue;
      const key="worker/"+w.id;
      live.add(key);
      const period=Math.max(30,w.heartbeat_interval_seconds||0);
      const {recent,prior}=this.intervals(key,w.last_heartbeat,{
        requests:m.http_requests, limited:m.http_statuses?.["429"]||0, errors:m.errors||0
      },period,m.uptime_seconds);
      if (recent?.limited>=5 && recent.limited>=recent.requests*0.2)
        alert("rate",label,recent.limited+" ответов 429 за "+Math.round(recent.seconds)+" с");
      if (recent?.errors>=3 && prior && recent.errors/recent.seconds>=2*prior.errors/prior.seconds)
        alert("errors",label,"растёт частота ошибок: "+recent.errors+" за "+Math.round(recent.seconds)+" с");
    }
    for (const j of jobs.filter(j=>j.status==="running")) {
      const label=(j.name||"Задание")+" #"+j.id, key="job/"+j.id;
      live.add(key);
      if (j.last_progress_at!=null && now-j.last_progress_at>=60)
        alert("stalled",label,"нет подтверждённого прогресса минимум 60 с");
      const {recent,prior}=this.intervals(key,now,{processed:j.processed});
      if (prior?.processed>=10 && recent.processed/recent.seconds<=0.5*prior.processed/prior.seconds)
        alert("speed",label,"скорость обработки снизилась минимум вдвое");
    }
    for (const key of this.series.keys()) if (!live.has(key)) this.series.delete(key);
    return out;
  }
}

if (typeof module!=="undefined") module.exports={LabMonitor};
