#!/usr/bin/env python3
"""Five-minute fixture walkthrough; requires a running Compose monitoring demo.

Creates/revokes only its own temporary key. No provider/API secret is printed.
"""
import json
import os
import urllib.error
import urllib.request
import subprocess

ROOT='http://localhost:8080'
TOKEN=os.getenv('GATEWAY_ADMIN_TOKEN','demo-local-admin-token')


def main():
    # Existing acceptance scripts verify streaming and metadata, key lifecycle,
    # MISS/HIT + concurrent coalescing, and monitored dashboard query health.
    for script,args in [('compose_acceptance.py',['normal']),('admin_demo.py',[]),('reuse_demo.py',[]),('monitoring_acceptance.py',[])]:
        subprocess.run([os.sys.executable,'scripts/'+script,*args],check=True)
    def admin(method,path,payload=None):
        request=urllib.request.Request(ROOT+path,method=method,data=json.dumps(payload).encode() if payload else None,headers={'Authorization':'Bearer '+TOKEN,'Content-Type':'application/json'})
        with urllib.request.urlopen(request,timeout=5) as response:
            data=response.read();return json.loads(data) if data else None
    key=admin('POST','/admin/keys',{'name':'reviewer-token-denial','aliases':['default-chat'],'capacity':1,'refill_per_second':1,'max_output_tokens':1,'allow_reuse':False})
    try:
        request=urllib.request.Request(ROOT+'/v1/chat/completions',data=json.dumps({'model':'default-chat','messages':[{'role':'user','content':'token bound'}],'max_tokens':1}).encode(),headers={'Authorization':'Bearer '+key['key'],'Content-Type':'application/json'})
        try:
            with urllib.request.urlopen(request,timeout=5) as response:raise AssertionError('oversized reservation admitted')
        except urllib.error.HTTPError as error:
            with error:
                assert error.code==429
                assert json.load(error)['error']['code']=='rate_limit_exceeded'
    finally:admin('DELETE','/admin/keys/'+key['id'])
    print('Reviewer token-budget rejection passed. Dashboard: http://localhost:3000/d/llm-gateway')
    print('For fallback/drop/disconnect and real Redis interruption run the resilience steps in README.rst; benchmarks/profiles contain the recorded evidence.')

if __name__=='__main__':main()
