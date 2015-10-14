#!/usr/bin/env python3
'''
Patroni Command Line Client
'''

import click
import os
import yaml
import logging

# from patroni.dcs import AbstractDCS, Cluster, Failover, Leader, Member

CONFIG_DIR_PATH = click.get_app_dir('patroni')
CONFIG_FILE_PATH = os.path.join(CONFIG_DIR_PATH, 'patronicli.yaml')
LOGLEVEL = 'DEBUG'


def load_config(path):
    logging.debug('Loading configuration from file {}'.format(path))
    config = None
    try:
        with open(path, 'rb') as fd:
            config = yaml.safe_load(fd)
    except:
        logging.exception('Could not load configuration file')
    return config or {}


def store_config(config, path):
    dir_path = os.path.dirname(path)
    if dir_path:
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)
    with open(path, 'w') as fd:
        yaml.dump(config, fd)


@click.group()
@click.option('--config-file', '-c', help='Use alternative configuration file',
              default=CONFIG_FILE_PATH)
@click.pass_context
def cli(ctx, config_file):
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=LOGLEVEL)
    ctx.obj = load_config(CONFIG_FILE_PATH)


@cli.command('list')
@click.pass_obj
def list_haclusters(obj):
    pass
