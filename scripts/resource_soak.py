#!/usr/bin/env python3
"""Repeated sustained cancellation and slow-reader resource acceptance."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import time
import urllib.request
from urllib.parse import urlsplit

from sdk_integration import ROOT, running, get_json
from load_baseline import metrics, one_request, process_stats, machine_details
from release_load import config

ACTIVE=('llmgw_active_attempts','llmgw_active_streams','llmgw_active_work_slots','llmgw_shared_executions','llmgw_shared_waiters','llmgw_usage_queue_depth','llmgw_usage_in_flight')
LOSS=('llmgw_usage_dropped_total','llmgw_usage_write_failures_total','llmgw_accounting_errors_total')


def sockets(pid):
    directory=Path(f'/proc/{pid}/fd')
    if directory.exists():
        return sum(os.readlink(p).startswith('socket:') for p in directory.iterdir() if p.exists())
    output=subprocess.run(['lsof','-nP','-a','-p',str(pid),'-iTCP','-sTCP:ESTABLISHED','-Ff'],text=True,capture_output=True,check=False)
    if output.returncode not in (0,1):raise RuntimeError('cannot measure process TCP sockets')
    return sum(line.startswith('f') for line in output.stdout.splitlines())


def drained(stats,timeout=5):
    end=time.monotonic()+timeout
    while True:
        sample=metrics(stats['management'])
        if all(sample.get(name,0)==0 for name in ACTIVE):return sample
        if time.monotonic()>end:raise AssertionError('active work did not drain')
        time.sleep(.02)


def retained(stats):
    # A post-GC heap sample separates retained data from ordinary allocation/GC
    # sawtooth. Never use this forced-GC run as a throughput benchmark.
    with urllib.request.urlopen(stats['management']+'/debug/pprof/heap?gc=1',timeout=5) as response:response.read()
    return metrics(stats['management'])


def sustained(seconds,concurrency):
    with running(('-chunk-delay','100ms'),configure=config,profiling=True) as (client,gateway,stats):
        address=urlsplit(str(client.base_url))
        payload=json.dumps({'model':'default-chat','messages':[{'role':'user','content':'sustained cancellation soak'}],'stream':True,'max_tokens':16})
        connections=[http.client.HTTPConnection(address.hostname,address.port,timeout=5) for _ in range(concurrency)]
        expected_canceled=0;samples=[];batches=0;completed=0;start=time.monotonic();next_sample=start
        before=metrics(stats['management']);baseline=None
        max_cancellation_seconds=0
        try:
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                while time.monotonic()-start<seconds or batches<3:
                    def worker(index):
                        cancel=index%2==0
                        one_request(connections[index],payload,cancel=cancel)
                        return time.monotonic() if cancel else None
                    closed=list(pool.map(worker,range(concurrency)))
                    expected_canceled+=(concurrency+1)//2
                    completed+=concurrency//2;batches+=1
                    earliest=min(value for value in closed if value is not None)
                    while get_json(stats['primary'])['canceled']<expected_canceled:
                        if time.monotonic()-earliest>=1:raise AssertionError('upstream cancellation exceeded one second')
                        time.sleep(.005)
                    max_cancellation_seconds=max(max_cancellation_seconds,time.monotonic()-earliest)
                    sample=drained(stats)
                    assert all(sample.get(name,0)==before.get(name,0) for name in LOSS),'accounting loss'
                    if time.monotonic()>=next_sample or time.monotonic()-start>=seconds:
                        sample=retained(stats)
                        entry={'elapsed_seconds':time.monotonic()-start,'goroutines':sample['go_goroutines'],'post_gc_heap_bytes':sample['go_memstats_heap_alloc_bytes'],'sockets':sockets(gateway.pid),'rss_bytes':process_stats(gateway.pid)['rss_bytes']}
                        if baseline is None and batches>=3:baseline=entry
                        if baseline:
                            # Regression guards based on bounded workers/pools, not
                            # a claim that a finite soak proves absence of leaks.
                            assert entry['goroutines']<=baseline['goroutines']+2*concurrency+32,entry
                            assert entry['sockets']<=baseline['sockets']+2*concurrency+8,entry
                            assert entry['post_gc_heap_bytes']<=baseline['post_gc_heap_bytes']+(16<<20),entry
                        samples.append(entry);next_sample=time.monotonic()+5
        finally:
            for connection in connections:connection.close()
        drained(stats)
        return {'duration_seconds':time.monotonic()-start,'concurrency':concurrency,'batches':batches,'completed_streams':completed,'canceled_streams':expected_canceled,'max_batch_cancellation_observation_seconds':max_cancellation_seconds,'samples':samples,'loss_deltas':{name:metrics(stats['management']).get(name,0)-before.get(name,0) for name in LOSS},'guards':{'goroutines_above_warm':2*concurrency+32,'sockets_above_warm':2*concurrency+8,'post_gc_heap_above_warm_bytes':16<<20}}


def slow_readers(rounds):
    def configure(c):
        config(c);c['write_timeout']='200ms';c['max_concurrent']=c['max_streams']=16
    with running(('-chunks','128','-payload','x'*32768,'-first-delay','0ms','-chunk-delay','0ms'),configure=configure) as (client,gateway,stats):
        target=urlsplit(str(client.base_url));samples=[]
        payload=json.dumps({'model':'default-chat','messages':[{'role':'user','content':'slow socket'}],'stream':True,'max_tokens':256})
        baseline=metrics(stats['management'])
        for index in range(rounds):
            def connect(_):
                connection=http.client.HTTPConnection(target.hostname,target.port,timeout=5)
                connection.connect();connection.sock.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,1024)
                connection.request('POST','/v1/chat/completions',payload,{'Content-Type':'application/json','Authorization':'Bearer sdk-test-key'})
                response=connection.getresponse();assert response.status==200
                return connection,response
            with ThreadPoolExecutor(max_workers=8) as pool:clients=list(pool.map(connect,range(8)))
            try:
                start=time.monotonic();sample=drained(stats,timeout=3)
                samples.append({'round':index+1,'drain_seconds_after_headers':time.monotonic()-start,'heap_bytes':sample['go_memstats_heap_alloc_bytes'],'goroutines':sample['go_goroutines']})
                assert all(sample.get(name,0)==baseline.get(name,0) for name in LOSS)
            finally:
                for connection,response in clients:response.close();connection.close()
        return {'rounds':rounds,'concurrency':8,'fixture_output_bytes_per_stream':128*32768,'receive_buffer_bytes':1024,'write_timeout_ms':200,'samples':samples,'assertion':'Active attempts, streams, work and usage drained while all client sockets remained open and response bodies were unread.'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds',type=float,default=120)
    parser.add_argument('--runs',type=int,default=3)
    parser.add_argument('--concurrency',type=int,default=32)
    parser.add_argument('--slow-rounds',type=int,default=10)
    parser.add_argument('--output',type=Path,default=ROOT/'tmp/resource-soak.json')
    args=parser.parse_args()
    if not(1<=args.seconds<=3600 and 1<=args.runs<=10 and 2<=args.concurrency<=100 and 1<=args.slow_rounds<=50):parser.error('invalid soak bounds')
    result={'revision':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'environment':{'hardware':machine_details(),'platform':platform.platform(),'go':subprocess.check_output(['go','version'],text=True).strip()},'runs':[],'limitations':'Finite developer-host soak, post-GC retained heap, warm idle connection pools allowed. Regression guards do not prove leak freedom.'}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    for index in range(args.runs):
        result['runs'].append(sustained(args.seconds,args.concurrency));args.output.write_text(json.dumps(result,indent=2)+'\n');print('Sustained run',index+1,'passed',flush=True)
    result['slow_readers']=slow_readers(args.slow_rounds)
    args.output.write_text(json.dumps(result,indent=2)+'\n');print('Slow readers passed; recorded',args.output)

if __name__=='__main__':main()
