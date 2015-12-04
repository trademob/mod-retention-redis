#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2009-2012:
#    Gabes Jean, naparuba@gmail.com
#    Gerhard Lausser, Gerhard.Lausser@consol.de
#    Gregory Starck, g.starck@gmail.com
#    Hartmut Goebel, h.goebel@goebel-consult.de
#
# This file is part of Shinken.
#
# Shinken is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Shinken is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Shinken.  If not, see <http://www.gnu.org/licenses/>.

# This Class is an example of an Scheduler module
# Here for the configuration phase AND running one

'''
    The `retention-redis` is used to keep track of shinken retention data between restarts.
    Therefore it saves this data to redis.

    The data in redis looks like:

    HOST-<hostname> (type: String) :                            (cPickle data of the host)
    SERVICE-<hostname>-<service-description> (type: String) :   (cPickle data of the service)
'''

try:
    import redis
except ImportError:
    redis = None
import cPickle

from shinken.basemodule import BaseModule
from shinken.log import logger

properties = {
    'daemons': ['scheduler'],
    'type': 'redis_retention',
    'external': False
}


def get_instance(plugin):
    '''
    Called by the plugin manager to get a broker
    '''
    logger.debug("Get a redis retention scheduler module for plugin %s" % plugin.get_name())
    if not redis:
        logger.error('Missing the module python-redis. Please install it.')
        raise Exception
    server = plugin.server
    instance = RedisRetentionScheduler(plugin, server)
    return instance


class RedisRetentionScheduler(BaseModule):
    '''
        The RedisRetentionScheduler saves retention data to redis. It listens on
        `save_retention` and `load_retention` events using the hook_* functions.
    '''
    def __init__(self, modconf, server):
        BaseModule.__init__(self, modconf)
        self.server = server
        self.client = None # will be initialized later when scheduler calls `init`

    def init(self):
        '''
        Called by Scheduler to say 'let's prepare yourself guy'
        '''
        logger.debug('[RedisRetention] Initialization of the redis module')
        self.client = redis.Redis(self.server)

    def hook_save_retention(self, daemon):
        '''
        Hook point for saving retention data.
        '''
        logger.debug('[RedisRetention] asking me to update the retention objects')

        # get all retention data from the daemon and persist it
        all_data = daemon.get_retention_data()

        for host_name, host in all_data['hosts'].iteritems():
            key = "HOST-%s" % host_name
            val = cPickle.dumps(host)
            self.client.set(key, val)

        for (host_name, service_desc), service in all_data['services'].iteritems():
            key = "SERVICE-%s,%s" % (host_name, service_desc)
            # space are not allowed in redis key.. so change it by SPACE token
            key = key.replace(' ', 'SPACE')
            val = cPickle.dumps(service)
            self.client.set(key, val)
        logger.info('Retention information updated in Redis')

    def hook_load_retention(self, daemon):
        '''
        Hook point for loading retention data. Final data should be passed to
        `daemon.restore_retention_data`.
        '''
        logger.debug('[RedisRetention] asking me to load the retention objects')

        # We got list of loaded data from retention server
        ret_hosts = {}
        ret_services = {}

        # We must load the data and format as the scheduler want :)
        for host in daemon.hosts:
            key = "HOST-%s" % host.host_name
            val = self.client.get(key)
            if val is not None:
                val = cPickle.loads(val)
                ret_hosts[host.host_name] = val

        for service in daemon.services:
            key = "SERVICE-%s,%s" % (service.host.host_name, service.service_description)
            # space are not allowed in redis key.. so change it by SPACE token
            key = key.replace(' ', 'SPACE')
            val = self.client.get(key)
            if val is not None:
                val = cPickle.loads(val)
                ret_services[(service.host.host_name, service.service_description)] = val

        all_data = {'hosts': ret_hosts, 'services': ret_services}

        # Ok, now comme load them scheduler :)
        daemon.restore_retention_data(all_data)

        logger.info('[RedisRetention] Retention objects loaded successfully.')
