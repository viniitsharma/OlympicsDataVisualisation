import os

import logging
import pytest # type: ignore
from pyfakefs.fake_filesystem_unittest import Patcher # type: ignore
from Utils.DataOperation import DataOperation

@pytest.fixture
def setup_logging(caplog):
    caplog.set_level(logging.INFO)
    return caplog

def test_pycache_cleanup(setup_logging):
    with Patcher() as patcher:
        # Create a fake directory structure
        patcher.fs.create_dir('/testdir/__pycache__')
        patcher.fs.create_file('/testdir/__pycache__/tempfile.pyc')

        # Change to the fake directory
        os.chdir('/testdir')

        # Verify that __pycache__ directory exists before cleanup
        assert os.path.exists('/testdir/__pycache__')

        # Call pycacheCleanup method
        DataOperation().pycacheCleanup()
        
        # Verify that __pycache__ directory has been removed
        assert not os.path.exists('/testdir/__pycache__')

