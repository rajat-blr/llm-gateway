#!/usr/bin/env python3
"""Five-minute fixture walkthrough; requires a running Compose monitoring demo.

Creates/revokes only its own temporary key. No provider/API secret is printed.
"""
import json
import os
import urllib.error
import urllib.request
import subprocess
from openai import APIError
from sdk_integration import running, get_json, wait_until

ROOT='http://localhost:8080'
TOKEN=os.getenv('GATEWAY_ADMIN_TOKEN','demo-local-admin-token')


def openai_fixture(config):
    backup=config['providers'].pop('backup-anthropic')
    backup['kind']='openai'
    config['providers'].pop('mock-anthropic')
    config['providers']['backup-openai']=backup
    config['routes'].pop('anthropic-chat')
    for target in config['routes']['default-chat']['targets']:
        if target['provider']=='backup-anthropic':target['provider']='backup-openai'


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
    messages=[{'role':'user','content':'reviewer failure scenario'}]
    with running(('-fail-every','1'),fallback=True,configure=openai_fixture) as (client,_,stats):
        response=client.chat.completions.with_raw_response.create(model='default-chat',messages=messages,max_tokens=16)
        response.parse()
        assert response.headers['X-Gateway-Provider']=='backup-openai'
        assert get_json(stats['primary'])['calls']==get_json(stats['secondary'])['calls']==1
    print('Isolated pre-commit failure selected backup-openai within two attempts.')
    with running(('-chunks','50','-chunk-delay','100ms'),configure=openai_fixture) as (client,_,stats):
        with client.chat.completions.create(model='default-chat',messages=messages,stream=True) as stream:
            for event in stream:
                if event.choices and event.choices[0].delta.content:break
        wait_until(lambda:get_json(stats['primary'])['canceled']==1,timeout=1)
    print('Isolated client disconnect canceled upstream within one second.')
    with running(('-drop-after','1'),fallback=True,configure=openai_fixture) as (client,_,stats):
        partial=''
        try:
            with client.chat.completions.create(model='default-chat',messages=messages,stream=True) as stream:
                for event in stream:
                    if event.choices:partial+=event.choices[0].delta.content or ''
        except APIError:pass
        else:raise AssertionError('dropped stream succeeded')
        assert partial and get_json(stats['primary'])['calls']==1 and get_json(stats['secondary'])['calls']==0
    print('Isolated mid-stream drop surfaced an SDK error without fallback.')
    print('Review benchmarks/profiles/investigation.rst and benchmarks/extended-method.rst for measured results and limits.')

if __name__=='__main__':main()
