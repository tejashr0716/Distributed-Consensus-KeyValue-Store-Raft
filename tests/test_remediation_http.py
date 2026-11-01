"""Real TCP tests. Run only against a disposable local stack. See README."""
import asyncio, os, uuid
from pathlib import Path
import httpx, pytest, asyncpg
import websockets
BASE=os.getenv('BASE_URL','http://127.0.0.1:8010')
SECOND=os.getenv('SECOND_BASE_URL',BASE)
def key(): return 'test:'+uuid.uuid4().hex
async def post(c,path,body,base=BASE):return await c.post(base+path,json=body)
async def acquire(c,k,client='a',lease=10000):
 r=await post(c,'/api/v1/locks/acquire',dict(lock_name=k,client_id=client,lease_ms=lease));assert r.status_code==200,r.text;return r.json()['fencing_token']
async def write(c,k,t,client='a',value='v'):
 return await post(c,'/api/v1/kv/set',dict(key=k,value=value,client_id=client,fencing_token=t))
async def release(c,k,t,client='a'):
 return await post(c,'/api/v1/locks/release',dict(lock_name=k,client_id=client,fencing_token=t))

@pytest.mark.asyncio
async def test_missing_row_acquisition_race_across_gateways():
 async with httpx.AsyncClient(timeout=20) as c:
  k=key();rs=await asyncio.gather(*(post(c,'/api/v1/locks/acquire',dict(lock_name=k,client_id=str(i),lease_ms=10000),BASE if i%2 else SECOND) for i in range(40)))
  assert sum(r.status_code==200 for r in rs)==1
  assert sum(r.status_code==409 for r in rs)==39

@pytest.mark.asyncio
async def test_no_token_bypass_or_wrong_owner():
 async with httpx.AsyncClient() as c:
  k=key();t=await acquire(c,k)
  assert (await write(c,k,t)).status_code==201
  assert (await write(c,k,None)).status_code==409
  assert (await write(c,k,t,'other')).status_code==409
  assert (await release(c,k,t,'other')).status_code==409
  assert (await write(c,k,t,value='next')).status_code==201

@pytest.mark.asyncio
async def test_stale_write_and_release_after_reacquisition():
 async with httpx.AsyncClient() as c:
  k=key();old=await acquire(c,k);assert (await write(c,k,old)).status_code==201
  assert (await release(c,k,old)).status_code==200
  new=await acquire(c,k,'b');assert new>old
  assert (await write(c,k,new,'b','new')).status_code==201
  assert (await write(c,k,old)).status_code==409
  assert (await release(c,k,old)).status_code==409
  assert (await c.get(BASE+'/api/v1/kv/'+k)).json()['value']=='new'

@pytest.mark.asyncio
async def test_new_key_concurrent_stale_and_current_writes():
 async with httpx.AsyncClient() as c:
  k=key();old=await acquire(c,k);await release(c,k,old);new=await acquire(c,k,'b')
  rs=await asyncio.gather(write(c,k,old),write(c,k,new,'b','current'))
  assert [r.status_code for r in rs]==[409,201]
  assert (await c.get(BASE+'/api/v1/kv/'+k)).json()['value']=='current'

@pytest.mark.asyncio
async def test_expiry_blocks_write_and_release():
 async with httpx.AsyncClient() as c:
  k=key();t=await acquire(c,k,lease=500);await asyncio.sleep(.65)
  assert (await write(c,k,t)).status_code==409
  assert (await release(c,k,t)).status_code==409
  assert await acquire(c,k,'b')>t

@pytest.mark.asyncio
async def test_native_utf8_limits_are_http_validation_errors():
 async with httpx.AsyncClient() as c:
  r=await post(c,'/api/v1/kv/set',dict(key='é'*40,value='v'))
  assert r.status_code==422

@pytest.mark.asyncio
async def test_websocket_receives_real_committed_write():
 async with websockets.connect(BASE.replace('http','ws')+'/ws/consensus') as ws:
  await ws.recv()
  async with httpx.AsyncClient() as c:
   k=key();r=await post(c,'/api/v1/kv/set',dict(key=k,value='websocket'))
   assert r.status_code==201
   import json
   event=json.loads(await asyncio.wait_for(ws.recv(),3));assert event['key']==k and event['type']=='KV_WRITE_COMMITTED'

@pytest.mark.asyncio
async def test_sequence_upgrade_above_existing_tokens():
 url=os.getenv('TEST_DATABASE_URL')
 if not url:pytest.skip('Requires explicit disposable TEST_DATABASE_URL')
 c=await asyncpg.connect(url)
 try:
  k=key();await c.execute("INSERT INTO distributed_locks(lock_name,holder_client_id,fencing_token,lease_expires_at,status) VALUES($1,'migration',900000,clock_timestamp(),'RELEASED')",k)
  async with c.transaction():await c.execute((Path(__file__).parents[1]/'sql/migrate_fencing.sql').read_text())
 finally:await c.close()
 async with httpx.AsyncClient() as client:assert await acquire(client,key())>900000
