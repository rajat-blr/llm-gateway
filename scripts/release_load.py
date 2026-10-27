#!/usr/bin/env python3
"""Extended local fixture matrix; writes every repetition and resource sample."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
import platform
import sqlite3
from pathlib import Path
import subprocess
import threading
import time
from urllib.parse import urlsplit

from sdk_integration import ROOT, running, admin
from load_baseline import stage, metrics, one_request, summarize, machine_details, redis_version


def config(c):
    c['token_limit'] = {'capacity': 1_000_000_000, 'refill_per_second': 1_000_000}
    c['max_concurrent'] = c['max_streams'] = 512
    c['sqlite']['usage_queue'] = 10000
    c['breaker']['failures'] = 1000


def coalesced(client, stats, concurrency, runs):
    key = admin(client, 'POST', '/admin/keys', {'name': 'coalesced-load', 'aliases': ['default-chat'], 'capacity': 1_000_000_000, 'refill_per_second': 1_000_000, 'max_output_tokens': 128, 'allow_reuse': True})
    url = urlsplit(str(client.base_url))
    results = []
    try:
        for run in range(runs):
            barrier = threading.Barrier(concurrency)
            payload = json.dumps({'model': 'default-chat', 'messages': [{'role': 'user', 'content': 'coalesced batch '+str(run)}], 'max_tokens': 16})
            before = metrics(stats['management'])
            def worker(_):
                connection = http.client.HTTPConnection(url.hostname, url.port, timeout=10)
                try:
                    barrier.wait(timeout=10)
                    start = time.perf_counter()
                    connection.request('POST', '/v1/chat/completions', payload, {'Content-Type':'application/json', 'Authorization':'Bearer '+key['key']})
                    response = connection.getresponse()
                    value = json.loads(response.read())
                    assert response.status == 200, value
                    assert value['choices'][0]['message']['content'] == 'hello '*3
                    return (time.perf_counter()-start)*1000, response.getheader('X-Cache')
                finally:
                    connection.close()
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                values = list(pool.map(worker, range(concurrency)))
            after = metrics(stats['management'])
            attempts = after.get('llmgw_attempts_total',0)-before.get('llmgw_attempts_total',0)
            states = dict(Counter(state for _,state in values))
            assert attempts == 1, (states, attempts)
            assert states.get('MISS') == 1 and states.get('COALESCED',0) > 0, states
            results.append({'scenario':'coalesced','concurrency':concurrency,'run':run+1,'latency':summarize([latency for latency,_ in values]),'cache_states':states,'upstream_attempts':attempts})
    finally:
        admin(client,'DELETE','/admin/keys/'+key['id'])
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=ROOT/'tmp/release-load.json')
    parser.add_argument('--runs',type=int,default=3)
    parser.add_argument('--seconds',type=float,default=2)
    parser.add_argument('--max-concurrency',type=int,choices=[100,500],default=500)
    args=parser.parse_args()
    if not (1<=args.runs<=5 and .1<=args.seconds<=10):parser.error('runs 1..5; seconds .1..10')
    result={'schema':1,'revision':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'environment':{'hardware':machine_details(),'platform':platform.platform(),'go':subprocess.check_output(['go','version'],text=True).strip(),'python':platform.python_version()},'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'binary_sha256':hashlib.sha256((ROOT/'bin/gateway').read_bytes()).hexdigest(),'runs':args.runs,'stage_seconds':args.seconds,'measurement':'closed loop; warm connection per worker; order alternates; all raw repetitions; sampled peaks; CPU/RSS include shared host limits','stages':[]}
    def run_case(name,mock_args=(),levels=(10,100),modes=('json',),reuse=None,fallback=False,large=False,expected_failure=False, db_contention=False):
        with running(mock_args,configure=config,fallback=fallback) as (client,gateway,stats):
            result['environment']['redis_version']=redis_version(json.loads(Path(stats['config']).read_text())['redis']['address'])
            key=None
            if reuse:
                key=admin(client,'POST','/admin/keys',{'name':name,'aliases':['default-chat'],'capacity':1_000_000_000,'refill_per_second':1_000_000,'max_output_tokens':128,'allow_reuse':True})
            try:
                for concurrency in levels:
                    for mode in modes:
                        payload=json.dumps({'model':'default-chat','messages':[{'role':'user','content':'x'*32768 if large else 'release-load'}],'max_tokens':128,'stream':mode=='stream'})
                        for run in range(args.runs):
                            targets=[('gateway',str(client.base_url))] if reuse or fallback or expected_failure or db_contention else [('direct',stats['primary'].removesuffix('/stats')),('gateway',str(client.base_url))]
                            if run%2:targets.reverse()
                            for target,url in targets:
                                lock=None; holder=None
                                if db_contention:
                                    lock=sqlite3.connect(stats['database'],check_same_thread=False)
                                    lock.execute('BEGIN IMMEDIATE')
                                    holder=threading.Thread(target=lambda: (time.sleep(1),lock.rollback(),lock.close()))
                                    holder.start()
                                sample=stage(url,payload,concurrency,args.seconds,stats['management'],gateway.pid,stats['primary_pid'],expected_text=('x'*4096)*8 if large else 'hello '*3,api_key=key['key'] if key else 'sdk-test-key',unique=reuse=='miss')
                                if holder:holder.join()
                                sample.update(scenario=name,target=target,mode=mode,concurrency=concurrency,run=run+1,expected_failure=expected_failure)
                                if expected_failure:
                                    assert sample['errors']>0 and sample['successful_requests']==0,sample
                                    assert all('stream error' in error for error in sample['error_examples']),sample
                                else:assert sample['errors']==0,sample
                                if reuse=='hit':assert sample['counter_deltas']['llmgw_attempts_total']==0,sample
                                if reuse=='miss':assert sample['counter_deltas']['llmgw_attempts_total']==sample['successful_requests'],sample
                                assert sample['counter_deltas']['llmgw_accounting_errors_total']==0,sample
                                if db_contention:assert sample['counter_deltas']['llmgw_usage_write_failures_total']>0,sample
                                result['stages'].append(sample)
                                args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(result,indent=2)+'\n')
                                print(name,target,mode,concurrency,run+1,'errors',sample['errors'],flush=True)
            finally:
                if key:admin(client,'DELETE','/admin/keys/'+key['id'])
    levels=(1,10,100,500) if args.max_concurrency==500 else (1,10,100)
    run_case('normal',levels=levels,modes=('json','stream'))
    run_case('large32KiB',('-chunks','8','-payload','x'*4096),modes=('json','stream'),large=True)
    run_case('cache-hit',reuse='hit')
    run_case('cache-miss',reuse='miss')
    run_case('sqlite-contention',levels=(10,),db_contention=True)
    run_case('seeded-fallback',('-fail-every','3'),fallback=True)
    run_case('midstream-drop',('-drop-after','1'),modes=('stream',),expected_failure=True)
    with running(('-first-delay','300ms'),configure=config) as (client,_,stats):
        result['coalesced']=coalesced(client,stats,100,args.runs)
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    print('Recorded',args.output)

if __name__=='__main__':main()
