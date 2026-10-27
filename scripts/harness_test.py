"""Correctness checks for test-only namespace cleanup and local env loading."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

import sdk_integration as sdk
from live_smoke import load_local_env


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
