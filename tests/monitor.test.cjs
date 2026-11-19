const test = require("node:test");
const assert = require("node:assert/strict");
const {LabMonitor} = require("../ui/monitor.js");

const job = patch => ({
  id:1, name:"Проверка", status:"running", worker_ids:[], processed:0, last_progress_at:1000,
  min_free_space_gb:2, ...patch
});
const worker = patch => ({
  id:"w", enabled:true, online:true, last_heartbeat:1000, heartbeat_interval_seconds:10,
  version:"1.0", expected_agent_version:"1.0", ...patch
});
const kinds = results => results.map(a=>a.kind).sort();

test("current thresholds, eligibility and offline distinction",()=>{
  const monitor=new LabMonitor();
  const w=worker({version:"0.9",metrics:{free_space_gb:1,ram_total_bytes:100,ram_used_bytes:90}});
  assert.deepEqual(kinds(monitor.evaluate([job({last_progress_at:940})],[w],1016)),
    ["disk","heartbeat","resources","stalled","version"]);
  assert.deepEqual(kinds(monitor.evaluate([job({status:"paused"})],[worker({online:false})],1200)),["offline"]);
  assert.deepEqual(kinds(monitor.evaluate([job({worker_ids:["other"]})],[worker({metrics:{free_space_gb:1}})],1000)),[]);
  assert.deepEqual(kinds(monitor.evaluate([], [worker({enabled:false,online:false})],1200)),[]);
});

test("rates use deltas in two windows, independent of accumulated totals",()=>{
  const monitor=new LabMonitor();
  const snap=(ts,requests,limited,errors,processed)=>monitor.evaluate(
    [job({processed,last_progress_at:ts})],[worker({last_heartbeat:ts,metrics:{
      http_requests:requests,http_statuses:{"429":limited},errors,uptime_seconds:ts-900
    }})],ts);
  assert.deepEqual(kinds(snap(1000,10000,1000,500,300)),[]);
  assert.deepEqual(kinds(snap(1030,10030,1001,501,330)),[]);
  assert.deepEqual(kinds(snap(1060,10050,1007,504,345)),["errors","rate","speed"]);
  assert.deepEqual(kinds(snap(1090,10100,1007,505,380)),[]);
});

test("duplicate heartbeat, restart, observation gap and removed entities reset or preserve series",()=>{
  const monitor=new LabMonitor();
  const snap=(ts,requests,uptime)=>monitor.evaluate([],[worker({last_heartbeat:ts,metrics:{
    http_requests:requests,errors:0,http_statuses:{"429":requests},uptime_seconds:uptime
  }})],ts);
  snap(1000,1,180);snap(1010,4,190);snap(1010,4,190);
  assert.equal(monitor.series.get("worker/w").length,2);
  assert.deepEqual(kinds(snap(1030,7,210)),["rate"]);
  assert.deepEqual(kinds(snap(1040,2,5)),[]);
  assert.deepEqual(kinds(snap(1200,70,165)),[]);
  monitor.evaluate([],[],1210);
  assert.equal(monitor.series.size,0);
});

test("normal change in duration does not trigger error growth or speed loss",()=>{
  const monitor=new LabMonitor();
  function sample(ts,processed,errors) {
    return monitor.evaluate([job({processed,last_progress_at:ts})],[worker({last_heartbeat:ts,
      metrics:{http_requests:processed,errors}})],ts);
  }
  sample(1000,0,0); sample(1030,30,2);
  assert.deepEqual(kinds(sample(1075,75,5)),[]);
});
