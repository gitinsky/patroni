import os
import pytest

from click.testing import CliRunner
from patroni.cli import cli, list_haclusters, store_config, load_config

CONFIG_FILE_PATH="./test-cli.yaml"

def test_rw_config():
    runner = CliRunner()
    with runner.isolated_filesystem():
        os.mkdir(CONFIG_FILE_PATH)
        with pytest.raises(Exception):
            result = load_config(CONFIG_FILE_PATH)
            assert 'Could not load configuration file' in result.output

        with pytest.raises(Exception):
            store_config(config, CONFIG_FILE_PATH)
        os.rmdir(CONFIG_FILE_PATH)

    config = "a:b"
    store_config(config, "abc/CONFIG_FILE_PATH")
    load_config(CONFIG_FILE_PATH)
  
def test_cli():
    runner = CliRunner()

    runner.invoke(cli, ['list'])

    result = runner.invoke(cli, ['--help'])
    assert 'Usage:' in result.output

def test_list_hacluster():
    runner = CliRunner()

    result = runner.invoke(list_haclusters)
    
