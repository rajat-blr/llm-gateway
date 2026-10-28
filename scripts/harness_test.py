"""Correctness checks for test-only namespace cleanup and local env loading."""
import os
import contextlib
import json
import sys
import live_smoke
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

import sdk_integration as sdk
from live_smoke import load_local_env, selected_providers, required_names, configure_providers


class HarnessTests(unittest.TestCase):
    def test_cleanup_keeps_neighbor_namespaces(self):
        config={'redis':{'address':os.getenv('REDIS_TEST_ADDR','127.0.0.1:6381'),'prefix':'sdk:'+uuid.uuid4().hex,'db':0}}
        prefix=config['redis']['prefix'];owned=prefix+':owned';foreign=prefix+'-other:sentinel'
        redis=sdk.TestRedis(config['redis'])
        try:
            redis.command('SET',owned,'fixture','PX',60000)
            redis.command('SET',foreign,'neighbor','PX',60000)
            sdk.cleanup_test_namespace(config)
            self.assertIsNone(redis.command('GET',owned))
            self.assertEqual(redis.command('GET',foreign),'neighbor')
        finally:
            redis.command('DEL',owned,foreign);redis.close()

    def test_cleanup_refuses_non_fixture_prefix(self):
        with patch.object(sdk,'TestRedis',side_effect=AssertionError('must not connect')):
            sdk.cleanup_test_namespace({'redis':{'prefix':'llmgw'}})

    def test_live_selection_needs_only_selected_credentials(self):
        self.assertEqual(required_names(selected_providers('openai')),['OPENAI_API_KEY','OPENAI_MODEL'])
        self.assertEqual(required_names(selected_providers('anthropic')),['ANTHROPIC_API_KEY','ANTHROPIC_MODEL'])
        self.assertEqual(len(required_names(selected_providers('both'))),4)
        for kind,alias in [('openai','default-chat'),('anthropic','anthropic-chat')]:
            with patch.dict(os.environ,{kind.upper()+'_MODEL':'fixture-model'},clear=True):
                config={}
                configure_providers(config,selected_providers(kind))
                self.assertEqual(list(config['providers']),[kind+'-live'])
                self.assertEqual(list(config['routes']),[alias])
                self.assertEqual(config['routes'][alias]['targets'][0]['model'],'fixture-model')
        with self.assertRaises(ValueError):selected_providers('unknown')

    def test_openai_only_smoke_uses_fixtures_without_anthropic_credentials(self):
        @contextlib.contextmanager
        def fixture_running(configure):
            def use_fixture(config):
                url=config['providers']['mock-openai']['base_url']
                configure(config)
                for provider in config['providers'].values():
                    provider['base_url']=url
                    provider['key_env']='MOCK_API_KEY'
            with sdk.running(configure=use_fixture) as fixture:
                yield fixture
                self.assertEqual(sdk.get_json(fixture[2]['primary'])['calls'],2)
                self.assertEqual(sdk.get_json(fixture[2]['secondary'])['calls'],0)
        environment={name:value for name,value in os.environ.items() if name not in live_smoke.NAMES}
        environment.update(OPENAI_API_KEY='fixture-only',OPENAI_MODEL='fixture-model')
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/'result.json'
            args=['live_smoke.py','--provider','openai','--live','--env-file',str(Path(directory)/'missing.env'),'--output',str(output)]
            with patch.dict(os.environ,environment,clear=True),patch.object(sys,'argv',args),patch.object(live_smoke,'running',fixture_running):
                live_smoke.main()
            report=json.loads(output.read_text())
            self.assertEqual(report['models'],{'openai':'fixture-model'})
            self.assertEqual(report['request_limit'],2)
            self.assertEqual(report['selected_providers'],['openai'])
            self.assertEqual(report['unverified_adapters'],['anthropic'])
            self.assertEqual([case['mode'] for case in report['cases']],['json','stream'])
            self.assertTrue(all(case['passed'] and case['adapter']=='openai' for case in report['cases']))

    def test_env_loading_preserves_existing_values_and_does_not_execute(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'.env'
            path.write_text('OPENAI_API_KEY=file-fixture\nexport ANTHROPIC_API_KEY="literal$(false)"\nOPENAI_MODEL="model with spaces"\nIGNORED_VARIABLE=unused\n')
            with patch.dict(os.environ,{'OPENAI_API_KEY':'existing-fixture'},clear=True):
                load_local_env(path)
                self.assertEqual(os.environ['OPENAI_API_KEY'],'existing-fixture')
                self.assertEqual(os.environ['ANTHROPIC_API_KEY'],'literal$(false)')
                self.assertEqual(os.environ['OPENAI_MODEL'],'model with spaces')
                self.assertNotIn('IGNORED_VARIABLE',os.environ)

if __name__=='__main__':unittest.main(verbosity=2)
