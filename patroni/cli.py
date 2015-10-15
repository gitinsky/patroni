#!/usr/bin/env python3
'''
Patroni Command Line Client
'''

import click
import os
import yaml
import json
import time
import datetime
from prettytable import PrettyTable
import logging

from patroni.etcd import Etcd

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

option_config_file = click.option('--config-file','-c', help='Alternative configuration file',
                                  default=CONFIG_FILE_PATH)
option_format = click.option('--format','-f', help='Output format (pretty, json)', default='pretty')
option_watchrefresh = click.option('-w', '--watch', type=click.IntRange(1, 300), 
                                    help='Auto update the screen every X seconds')
option_watch = click.option('-W', is_flag=True, help='Auto update the screen every 2 seconds')


@click.group()
@click.pass_context
def cli(ctx):
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=LOGLEVEL)

def get_dcs(config):
    if 'etcd' in config:
        return Etcd(None, config['etcd'])
    raise Exception('Can not find sutable configuration of distributed configuration store')

def print_output(columns, rows=[], alignment=None, format='pretty'):
    if format == 'pretty':
        t = PrettyTable(columns)
        for k, v in (alignment or {}).iteritems():
            t.align[k] = v
        for r in rows:
            t.add_row(r)
        print(t)
        return

    if format == 'json':
        elements = list()
        for r in rows:
            elements.append(dict(zip(columns,r)))

        print( json.dumps(elements) )

@cli.command('list', help='List the Patroni clusters')
@option_config_file
@option_format
def list_clusters(config_file, format):
    config   = load_config(config_file)
    dcs      = get_dcs(config)
    clusters = [[c] for c in dcs.list_clusters()]
   
    print_output(['Cluster'], clusters, {'Cluster':'l'}, format)

def get_cluster(dcs, scope):
    dcs._scope = scope
    cluster = dcs.get_cluster()

def watching(w, watch):
    if w and not watch:
        watch = 2
    if watch:
        click.clear()
    yield 0
    if watch:
        while True:
            time.sleep(watch)
            click.clear()
            yield 0

@cli.command('failover', help='Failover to a replica')
@click.argument('cluster_name')
@click.option('--leader', help='The name of the current master', default=None)
@click.option('--candidate', help='The name of the candidate', default=None)
@click.option('--force', is_flag=True)
@option_config_file
def failover(config_file, cluster_name, leader, candidate, force):
    """
        We want to trigger a failover for the specified cluster name.

        We verify that the cluster name, leader name and candidate name are correct.
        If so, we trigger a failover and keep the client up to date.
    """
    config = load_config(config_file)
    dcs = get_dcs(config)
    dcs._scope = cluster_name
    cluster = dcs.get_cluster()

    if cluster.leader is None:
        raise Exception('This cluster has no leader')

    if leader is None:
        leader = click.prompt('Leader', type=str, default=cluster.leader.member.name)

    if cluster.leader.member.name <> leader:
        raise Exception('Member {} is not the leader of cluster {}'.format(leader, cluster_name))

    member_names = [str(m.name) for m in cluster.members if m.name <> leader]
    member_names.sort()

    if candidate is None:
        candidate = click.prompt('Candidate '+str(member_names), type=str, default='')

    if candidate <> '' and not candidate in member_names:
        raise Exception('Member {} does not exist in cluster {}'.format(candidate, cluster_name))

    ## By now we have established that the leader exists and the candidate exists
    click.echo('Current cluster topology')
    output_members(dcs, [cluster_name])

    if not force:
        click.confirm('Are you sure you want to failover cluster {}, demoting current master {}?'.format(cluster_name, leader))

    failover_value = '{}:{}'.format(leader, candidate)

    dcs.set_failover_value(failover_value)
    click.echo(timestamp()+' Initialized failover from leader {}'.format(leader))
    ## The failover process should within a minute update the failover key, we will keep watching it until it changes
    ## or we timeout

    timeout = 20
    t_end = time.time() + timeout
    while time.time() < t_end:
        cluster = dcs.get_cluster()
        if cluster.failover is None:
            break
        time.sleep(1)

    if cluster.failover is not None:
        raise Exception('Failover key was not changed after {} seconds, the cluster seems unhealthy'.format(timeout))

    click.echo(timestamp()+' Failover key was changed')

    ## Now we wait for the leader to be known
    t_end = time.time() + timeout
    while time.time() < t_end:
        if cluster.leader is not None:
            break
        cluster = dcs.get_cluster()
        time.sleep(1)

    click.echo(timestamp()+' Failover complete, new leader is {}'.format(str(cluster.leader.member.name)))
    

def output_members(dcs, cluster_names, format='pretty'):
    if len(cluster_names) == 0:
        cluster_names = dcs.list_clusters()
        cluster_names.sort()

    rows = []
    for cn in cluster_names:
        dcs._scope = str(cn)
        cluster = dcs.get_cluster()
        logging.debug(cluster)
        leader_name = None
        if cluster.leader:
            leader_name = cluster.leader.member.name
   
        ## Mainly for pretty printing and watching we sort the output 
        cluster.members.sort(key=lambda x: x.name)
        for m in cluster.members:
            logging.debug(m) 
            role = 'replica'
            if m.name == leader_name:
                role = 'master'
            rows.append([cn, m.name, role])
        
    print_output(['Cluster', 'Member', 'Role'], rows, {'Cluster':'l', 'Member':'l', 'Role':'l'}, format)


@cli.command('members', help='List the Patroni members')
@click.argument('cluster_names', nargs=-1)
@option_config_file
@option_format
@option_watch
@option_watchrefresh
def members(config_file, cluster_names, format, watch, w):
    config = load_config(config_file)
    dcs = get_dcs(config)

    for _ in watching(w, watch):
        output_members(dcs,cluster_names,format)

def timestamp():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


@cli.command('configure', help='Create configuration file')
@click.option('--config-file','-c', help='Configuration file', prompt='Configuration file', default=CONFIG_FILE_PATH)
@click.option('--dcs','-d',  help='Distributed Configuration Store', prompt='DCS type', default='etcd')
@click.option('--connect','-s', help='DCS connect string', default='127.0.0.1:4001', prompt=True)
def configure(config_file, dcs, connect):
    config = dict()
    config[str(dcs)] = {'host':str(connect)}
    store_config(config, CONFIG_FILE_PATH)


    

