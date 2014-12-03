"""
Common utilities and constants used throughout the project.
"""

import ConfigParser
import json
import os
import shlex
import subprocess
import sys


settings = ConfigParser.SafeConfigParser()
settings.read('{}/settings.cfg'.format(os.path.dirname(os.path.realpath(__file__))))
CONF = dict(settings.items('setup'))
CONF['fast'] = settings.getboolean('setup', 'fast')
CONF.update(dict(settings.items('general')))
CONF['map_timeout'] = settings.getint('general', 'map_timeout')
CONF['apply_timeout'] = settings.getint('general', 'apply_timeout')
CONF['apply_tries'] = settings.getint('general', 'apply_tries')
CONF['compress_level'] = settings.getint('general', 'compress_level')
CONF.update(dict(settings.items('gcloud')))

SUCCESS = 0
ERR_UPD_FAIL = 1
RDY_MSG = b'READY'
END_MSG = b'END'


color2num = {'gray': 30, 'red': 31, 'green': 32, 'yellow': 33, 'blue': 34, 'magenta': 35, \
        'cyan': 36, 'white': 37, 'crimson': 38}

def colorize(string, color='red'):
    attr = []
    num = color2num[color]
    attr.append(str(num))
    return '\x1b[%sm%s\x1b[0m'%(';'.join(attr), string)    


def wf(msg):
    sys.stdout.write(msg)
    sys.stdout.flush()


def get_instances(cluster=None):
    insts = {}
    list_cmd = 'gcloud compute instances list --format json'
    if cluster is not None:
        list_cmd += ' --regexp {}-.*'.format(cluster)
    gc_output = subprocess.check_output(shlex.split(list_cmd), close_fds=True)
    infos = json.loads(gc_output)
    for info in infos:
        insts[bytes(info['name'])] = {'zone': str(info['zone']), 'status': str(info['status']), \
                'net_ip': str(info['networkInterfaces'][0]['networkIP']), \
                'ext_ip': str(info['networkInterfaces'][0]['accessConfigs'][0]['natIP'])}
    return insts


def get_clusters():
    clusters = {}
    list_cmd = 'gcloud compute instances list --format json'
    gc_output = subprocess.check_output(shlex.split(list_cmd), close_fds=True)
    infos = json.loads(gc_output)
    for info in infos:
        try:
            for kv in info['metadata']['items']:
                if kv['key'] == 'cluster':
                    cluster = kv['value']
                    if cluster not in clusters:
                        clusters[cluster] = 0
                    clusters[cluster] += 1
        except KeyError:
            continue
    return clusters


def to_start(cluster, num_insts):
    name = cluster.keys()[0].split('-')[0]
    insts = []
    num = 0
    while len(insts) < num_insts:
        inst = '{}-{:0=3d}'.format(name, num)
        if inst not in cluster:
            insts.append(inst)
        num += 1
    return insts


def to_kill(cluster, num_insts):
    return sorted(cluster, reverse=True)[:num_insts]

