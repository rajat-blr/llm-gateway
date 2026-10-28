#!/usr/bin/env python3
"""Explicitly enabled, small real-provider checks through the gateway.

Reads only named variables from environment or ignored .env; never prints
credentials, prompts, completions or upstream error bodies.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import shlex
import subprocess

from sdk_integration import ROOT, running

NAMES=('OPENAI_API_KEY','ANTHROPIC_API_KEY','OPENAI_MODEL','ANTHROPIC_MODEL')


def load_local_env(path):
    if not path.exists():return
    if path.stat().st_size>65536:raise ValueError('local env file is too large')
    for line in path.read_text().splitlines():
        line=line.strip()
        if not line or line.startswith('#'):continue
        if line.startswith('export '):line=line[7:]
        name,sep,raw=line.partition('=')
        name=name.strip()
        if name not in NAMES or not sep or os.getenv(name):continue
        parts=shlex.split(raw,comments=True)
        if len(parts)>1:raise ValueError('env values with spaces must be quoted')
        if parts:os.environ[name]=parts[0]


PROVIDERS={
    'openai': {'alias':'default-chat','base_url':'https://api.openai.com/v1','key_env':'OPENAI_API_KEY','model_env':'OPENAI_MODEL'},
    'anthropic': {'alias':'anthropic-chat','base_url':'https://api.anthropic.com/v1','key_env':'ANTHROPIC_API_KEY','model_env':'ANTHROPIC_MODEL'},
}


def selected_providers(choice):
    if choice=='both':return tuple(PROVIDERS)
    if choice not in PROVIDERS:raise ValueError('unsupported provider')
    return (choice,)


def required_names(providers):
    return [PROVIDERS[kind][field] for kind in providers for field in ('key_env','model_env')]


def configure_providers(config,providers):
    config['request_timeout']='60s';config['idle_timeout']='20s'
    config['providers']={kind+'-live':{'kind':kind,'base_url':PROVIDERS[kind]['base_url'],'key_env':PROVIDERS[kind]['key_env']} for kind in providers}
    config['routes']={PROVIDERS[kind]['alias']:{'revision':1,'targets':[{'provider':kind+'-live','model':os.environ[PROVIDERS[kind]['model_env']],'attempt_timeout':'55s'}],'max_output_tokens':64} for kind in providers}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--live',action='store_true',help='authorize two small requests per selected provider')
    parser.add_argument('--provider',choices=['openai','anthropic','both'],default='both')
    parser.add_argument('--env-file',type=Path,default=ROOT/'.env')
    parser.add_argument('--output',type=Path,default=ROOT/'tmp/live-smoke.json')
    args=parser.parse_args()
    load_local_env(args.env_file)
    providers=selected_providers(args.provider)
    missing=[name for name in required_names(providers) if not os.getenv(name)]
    if missing:raise SystemExit('Configure locally: '+', '.join(missing))
    if not args.live:raise SystemExit('Use --live to explicitly enable '+str(2*len(providers))+' provider requests')
    def configure(c):
        configure_providers(c,providers)
    report={'recorded_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'models':{kind:os.environ[PROVIDERS[kind]['model_env']] for kind in providers},'revision':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'cases':[],'request_limit':2*len(providers),'selected_providers':list(providers),'unverified_adapters':[kind for kind in PROVIDERS if kind not in providers],'max_output_tokens_per_request':64}
    try:
        with running(configure=configure) as (client,_,_):
            client.timeout=65
            for kind in providers:
                alias=PROVIDERS[kind]['alias']
                response=client.chat.completions.with_raw_response.create(model=alias,messages=[{'role':'user','content':'Reply with a short greeting.'}],max_tokens=64)
                value=response.parse()
                assert value.model==alias and value.choices[0].message.content
                assert value.usage and value.usage.total_tokens>=0
                assert response.headers.get('X-Gateway-Provider')==kind+'-live'
                report['cases'].append({'adapter':kind,'mode':'json','passed':True,'usage_known':True})
                text=False;finish=False;usage=False;identity=None
                with client.chat.completions.create(model=alias,messages=[{'role':'user','content':'Reply with a short greeting.'}],max_tokens=64,stream=True,stream_options={'include_usage':True}) as stream:
                    for event in stream:
                        assert event.model==alias
                        if identity is None:identity=event.id
                        assert event.id==identity
                        for choice in event.choices:
                            text=text or bool(choice.delta.content)
                            finish=finish or choice.finish_reason is not None
                        usage=usage or event.usage is not None
                assert text and finish and usage
                report['cases'].append({'adapter':kind,'mode':'stream','passed':True,'usage_known':True})
    except Exception as error:
        # Store sanitized classifications only; SDK text may contain headers.
        report['failure']={'type':type(error).__name__}
        args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2)+'\n')
        raise SystemExit('Real-provider smoke failed ('+type(error).__name__+'); inspect gateway configuration/account access locally.') from None
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(', '.join(providers)+' passed JSON and SSE live smoke checks; recorded',args.output)

if __name__=='__main__':main()
