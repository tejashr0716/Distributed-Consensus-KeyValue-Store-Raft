"""Real loopback TCP benchmark; disposable dataset; no implied production SLA."""
import asyncio, json, os, platform, statistics, time, uuid, math, subprocess
from pathlib import Path
from collections import Counter
from urllib.parse import urlparse
import httpx
PROJECT='chandystore'
DEFAULT_PORT=8010 if PROJECT=='chandystore' else 8011
BASE=os.getenv('BASE_URL',f'http://127.0.0.1:{DEFAULT_PORT}')
if urlparse(BASE).hostname not in ('127.0.0.1','localhost','::1'):
    raise SystemExit('Safety: benchmark accepts loopback targets only')
N=int(os.getenv('BENCH_REQUESTS','200')); CONCURRENCY=int(os.getenv('BENCH_CONCURRENCY','10'));TRIALS=3
async def main():
 raw=[]; summaries=[]; runid=uuid.uuid4().hex[:12]
 async with httpx.AsyncClient(base_url=BASE,timeout=30) as c:
  health=(await c.get('/health')).json()
  scenarios=['kv','locks','contention'] if PROJECT=='chandystore' else ['telemetry']
  for scenario in scenarios:
   for trial in range(TRIALS):
    sem=asyncio.Semaphore(CONCURRENCY); samples=[]
    shared=f'bench:{runid}:contend:{trial}'
    async def req(i,warmup=False):
     async with sem:
      key=f'bench:{runid}:{scenario}:{trial}:{i}'+(':w' if warmup else '')
      if scenario=='kv':path='/api/v1/kv/set';body={'key':key,'value':'benchmark','client_id':'bench'}
      elif scenario in ('locks','contention'):
       path='/api/v1/locks/acquire';body={'lock_name':shared if scenario=='contention' else key,'client_id':str(i),'lease_ms':60000}
      else:path='/api/v1/telemetry';body={'node_id':'node-us-east-1a','cpu_pct':30,'mem_pct':40,'disk_pct':50,'p99_latency_ms':20,'error_rate_pct':0.01}
      start=time.perf_counter()
      try:r=await c.post(path,json=body);code=r.status_code
      except httpx.HTTPError:code=0
      elapsed=(time.perf_counter()-start)*1000
      if not warmup:samples.append({'scenario':scenario,'trial':trial,'request':i,'status':code,'latency_ms':elapsed})
    if scenario!='contention':
     for i in range(10):await req(i,True)
    started=time.perf_counter();await asyncio.gather(*(req(i) for i in range(N)));duration=time.perf_counter()-started
    raw.extend(samples);ok=[x['latency_ms'] for x in samples if x['status'] in (200,201)];ok.sort()
    summary={'scenario':scenario,'trial':trial,'attempted':N,'successful':len(ok),'status_counts':dict(Counter(x['status'] for x in samples)),'duration_seconds':duration,'successful_requests_per_second':len(ok)/duration,
       'p50_success_ms':statistics.median(ok) if ok else None}
    for q,label in [(.95,'p95_success_ms'),(.99,'p99_success_ms')]:summary[label]=ok[math.ceil(len(ok)*q)-1] if ok else None
    if scenario=='contention':summary['one_holder_observed']=(len(ok)==1 and all(x['status'] in (200,409) for x in samples))
    summaries.append(summary);print(json.dumps(summary),flush=True)
 report={'project':PROJECT,'base_url':BASE,'boundary':'client HTTP POST/full response over loopback TCP to Uvicorn; includes API work, not pure network latency','platform':platform.platform(),'python':platform.python_version(),'concurrency':CONCURRENCY,'trials':TRIALS,'warmups_per_trial_except_contention':10,'health':health,'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).resolve().parents[1],text=True).strip(),'working_tree_dirty':bool(subprocess.check_output(['git','status','--porcelain'],cwd=Path(__file__).resolve().parents[1])),'summaries':summaries,'raw_samples':raw,'caveat':'Synthetic local workload; data and unique lock rows are retained. Broker state is reported; no durable message-delivery guarantee.'}
 out=Path(__file__).resolve().parents[1]/'reports/real-http-benchmark.json';out.parent.mkdir(exist_ok=True);out.write_text(json.dumps(report,indent=2))
if __name__=='__main__':asyncio.run(main())
