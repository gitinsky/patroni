import logging
import random
import requests
import time

from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import NoNodeError, NodeExistsError
from patroni.dcs import AbstractDCS, Cluster, Failover, Leader, Member
from patroni.exceptions import DCSError
from patroni.utils import sleep
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)


class ZooKeeperError(DCSError):
    pass


class ExhibitorEnsembleProvider:

    TIMEOUT = 3.1

    def __init__(self, hosts, port, uri_path='/exhibitor/v1/cluster/list', poll_interval=300):
        self._exhibitor_port = port
        self._uri_path = uri_path
        self._poll_interval = poll_interval
        self._exhibitors = hosts
        self._master_exhibitors = hosts
        self._zookeeper_hosts = ''
        self._next_poll = None
        while not self.poll():
            logger.info('waiting on exhibitor')
            sleep(5)

    def poll(self):
        if self._next_poll and self._next_poll > time.time():
            return False

        json = self._query_exhibitors(self._exhibitors)
        if not json:
            json = self._query_exhibitors(self._master_exhibitors)

        if isinstance(json, dict) and 'servers' in json and 'port' in json:
            self._next_poll = time.time() + self._poll_interval
            zookeeper_hosts = ','.join([h + ':' + str(json['port']) for h in sorted(json['servers'])])
            if self._zookeeper_hosts != zookeeper_hosts:
                logger.info('ZooKeeper connection string has changed: %s => %s', self._zookeeper_hosts, zookeeper_hosts)
                self._zookeeper_hosts = zookeeper_hosts
                self._exhibitors = json['servers']
                return True
        return False

    def _query_exhibitors(self, exhibitors):
        random.shuffle(exhibitors)
        for host in exhibitors:
            uri = 'http://{}:{}{}'.format(host, self._exhibitor_port, self._uri_path)
            try:
                response = requests.get(uri, timeout=self.TIMEOUT)
                return response.json()
            except RequestException:
                pass
        return None

    @property
    def zookeeper_hosts(self):
        return self._zookeeper_hosts


class ZooKeeper(AbstractDCS):

    def __init__(self, name, config):
        super(ZooKeeper, self).__init__(name, config)

        hosts = config.get('hosts', [])
        if isinstance(hosts, list):
            hosts = ','.join(hosts)

        self.exhibitor = None
        if 'exhibitor' in config:
            exhibitor = config['exhibitor']
            interval = exhibitor.get('poll_interval', 300)
            self.exhibitor = ExhibitorEnsembleProvider(exhibitor['hosts'], exhibitor['port'], poll_interval=interval)
            hosts = self.exhibitor.zookeeper_hosts

        self.client = KazooClient(hosts=hosts,
                                  timeout=(config.get('session_timeout', None) or 30),
                                  command_retry={
                                      'deadline': (config.get('reconnect_timeout', None) or 10),
                                      'max_delay': 1,
                                      'max_tries': -1},
                                  connection_retry={'max_delay': 1, 'max_tries': -1})
        self.client.add_listener(self.session_listener)

        self._my_member_data = None
        self.fetch_cluster = True
        self.last_leader_operation = 0

        self.client.start(None)

    def session_listener(self, state):
        if state in [KazooState.SUSPENDED, KazooState.LOST]:
            self.cluster_watcher(None)

    def cluster_watcher(self, event):
        self.fetch_cluster = True
        self.event.set()

    def get_node(self, key, watch=None):
        try:
            ret = self.client.get(key, watch)
            return (ret[0].decode('utf-8'), ret[1])
        except NoNodeError:
            return None

    @staticmethod
    def member(name, value, znode):
        return Member.from_node(znode.version, name, znode.ephemeralOwner, value)

    def get_children(self, key, watch=None):
        try:
            return self.client.get_children(key, watch)
        except NoNodeError:
            return []

    def load_members(self):
        members = []
        for member in self.get_children(self.members_path, self.cluster_watcher):
            data = self.get_node(self.members_path + member)
            if data is not None:
                members.append(self.member(member, *data))
        return members

    def _inner_load_cluster(self):
        self.fetch_cluster = False
        self.event.clear()
        nodes = set(self.get_children(self.client_path(''), self.cluster_watcher))
        if not nodes:
            self.fetch_cluster = True

        # get initialize flag
        initialize = self._INITIALIZE in nodes

        # get list of members
        members = self.load_members() if self._MEMBERS[:-1] in nodes else []

        # get leader
        leader = self.get_node(self.leader_path) if self._LEADER in nodes else None
        if leader:
            client_id = self.client.client_id
            if leader[0] == self._name and client_id is not None and client_id[0] != leader[1].ephemeralOwner:
                logger.info('I am leader but not owner of the session. Removing leader node')
                self.client.delete(self.leader_path)
                leader = None

            if leader:
                member = Member(-1, leader[0], None, {})
                member = ([m for m in members if m.name == leader[0]] or [member])[0]
                leader = Leader(leader[1].version, leader[1].ephemeralOwner, member)
                self.fetch_cluster = member.index == -1

        # failover key
        failover = self.get_node(self.failover_path, watch=self.cluster_watcher) if self._FAILOVER in nodes else None
        if failover:
            failover = Failover.from_node(failover[1].version, failover[0])

        # get last leader operation
        optime = self.get_node(self.leader_optime_path) if self._OPTIME in nodes and self.fetch_cluster else None
        self.last_leader_operation = 0 if optime is None else int(optime[0])
        self._cluster = Cluster(initialize, leader, self.last_leader_operation, members, failover)

    def _load_cluster(self):
        if self.exhibitor and self.exhibitor.poll():
            self.client.set_hosts(self.exhibitor.zookeeper_hosts)

        if self.fetch_cluster:
            try:
                self.client.retry(self._inner_load_cluster)
            except:
                logger.exception('get_cluster')
                self.session_listener(KazooState.LOST)
                raise ZooKeeperError('ZooKeeper in not responding properly')

    def _create(self, path, value, **kwargs):
        try:
            self.client.retry(self.client.create, path, value.encode('utf-8'), **kwargs)
            return True
        except:
            return False

    def attempt_to_acquire_leader(self):
        ret = self._create(self.leader_path, self._name, makepath=True, ephemeral=True)
        ret or logger.info('Could not take out TTL lock')
        return ret

    def set_failover_value(self, value, index=None):
        try:
            self.client.retry(self.client.set, self.failover_path, value.encode('utf-8'), version=index or -1)
            return True
        except NoNodeError:
            return value == '' or (not index and self._create(self.failover_path, value.encode('utf-8')))
        except:
            logging.exception('set_failover_value')
            return False

    def initialize(self):
        return self._create(self.initialize_path, self._name, makepath=True)

    def touch_member(self, data, ttl=None):
        cluster = self.cluster
        me = cluster and ([m for m in cluster.members if m.name == self._name] or [None])[0]
        path = self.member_path
        data = data.encode('utf-8')
        create = not me
        if me and self.client.client_id is not None and me.session != self.client.client_id[0]:
            try:
                self.client.retry(self.client.delete, path)
            except NoNodeError:
                pass
            except:
                return False
            create = True

        if not create and data == self._my_member_data:
            return True

        try:
            if create:
                self.client.retry(self.client.create, path, data, makepath=True, ephemeral=True)
            else:
                self.client.retry(self.client.set, path, data)
            self._my_member_data = data
            return True
        except NodeExistsError:
            try:
                self.client.retry(self.client.set, path, data)
                self._my_member_data = data
                return True
            except:
                logger.exception('touch_member')
        except:
            logger.exception('touch_member')
        return False

    def take_leader(self):
        return self.attempt_to_acquire_leader()

    def write_leader_optime(self, last_operation):
        last_operation = last_operation.encode('utf-8')
        if last_operation != self.last_leader_operation:
            self.last_leader_operation = last_operation
            path = self.leader_optime_path
            try:
                self.client.retry(self.client.set, path, last_operation)
            except NoNodeError:
                try:
                    self.client.retry(self.client.create, path, last_operation, makepath=True)
                except:
                    logger.exception('Failed to create %s', path)
            except:
                logger.exception('Failed to update %s', path)

    def update_leader(self):
        return True

    def delete_leader(self):
        self.client.restart()
        self._my_member_data = None
        return True

    def _cancel_initialization(self):
        node = self.get_node(self.initialize_path)
        if node and node[0] == self._name:
            self.client.delete(self.initialize_path, version=node[1].version)

    def cancel_initialization(self):
        try:
            self.client.retry(self._cancel_initialization)
        except:
            logger.exception("Unable to delete initialize key")

    def watch(self, timeout):
        if super(ZooKeeper, self).watch(timeout):
            self.fetch_cluster = True
        return self.fetch_cluster
