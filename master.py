#!/usr/bin/env python
"""
Main cluster control functionality, for starting, stopping, updating, and resetting clusters.
"""

import shlex
import socket
import string
import subprocess
import sys
import xmlrpclib

from common import *


START_CMD = """gcloud compute instances create --project {} {{}} --zone {} --machine-type {} \
        --network "default" --scopes userinfo-email compute-rw storage-full \
        --tags http-server https-server --image "{}" --metadata cluster={{}} \
        --metadata-from-file startup-script={}""".format(CONF['project'], CONF['zone'], \
        CONF['machine_type'], CONF['image'], CONF['startup_script'])
KILL_CMD = """gcloud compute instances delete {{}} --zone {} --quiet \
        --delete-disks all""".format(CONF['zone'])


def start_cluster(name, size):
    if get_instances(name):
        print(colorize("Cluster '{}' already exists. Did you mean resizecluster?".format(name)))
        return
    insts_str = ' '.join(['{}-{:0=3d}'.format(name, i) for i in range(size)])
    print("Starting cluster '{}'...".format(name))
    cmd = shlex.split(START_CMD.format(insts_str, name))
    start = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    stdoutdata, stderrdata = start.communicate()
    if start.returncode != 0:
        print(colorize('Error occurred when starting cluster. Output below:'))
        print(stderrdata)
    start.stderr.close()
    start.stdout.close()


def kill_cluster(name):
    cluster = get_instances(name)
    if not cluster:
        print(colorize("Cluster '{}' does not exist. Did you mean startcluster?".format(name)))
        return
    insts_str = ' '.join(cluster)
    print("Shutting down cluster '{}'...".format(name))
    cmd = shlex.split(KILL_CMD.format(insts_str))
    kill = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    stdoutdata, stderrdata = kill.communicate()
    if kill.returncode != 0:
        print(colorize('Error occurred when shutting down cluster. Output below:'))
        print(stderrdata)
    kill.stderr.close()
    kill.stdout.close()


def resize_cluster(name, size):
    cluster = get_instances(name)
    if not cluster:
        print(colorize("Cluster '{}' does not exist. Did you mean startcluster?".format(name)))
        return
    if len(cluster) < size:
        insts = to_start(cluster, size - len(cluster))
        print("Starting {} instances for cluster '{}'...".format(len(insts), name))
        cmd = shlex.split(START_CMD.format(' '.join(insts), name))
        resize = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        reset_cluster(name, cluster=cluster)
        update_cluster(name, cluster=cluster)
    elif len(cluster) > size:
        insts = to_kill(cluster, len(cluster) - size)
        print("Shutting down {} instances from cluster '{}'...".format(len(insts), name))
        cmd = shlex.split(KILL_CMD.format(' '.join(insts)))
        resize = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    else:
        return
    stdoutdata, stderrdata = resize.communicate()
    if resize.returncode != 0:
        print(colorize('Error occurred when resizing cluster. Output below:'))
        print(stderrdata)
    resize.stderr.close()
    resize.stdout.close()


def update_cluster(name, cluster=None):
    if cluster is None:
        cluster = get_instances(name)
    if not cluster:
        print(colorize("Cluster '{}' does not exist. Did you mean startcluster?".format(name)))
        return
    proxies = {}
    for inst, info in cluster.items():
        proxies[inst] = xmlrpclib.ServerProxy('http://{}:8000'.format(info['ext_ip']))
    print("Updating cluster '{}'...".format(name))
    for inst, proxy in proxies.items():
        try:
            rc = proxy.update()
        except socket.error:
            print colorize("WARNING: could not connect to '{}'".format(inst))
            continue  # TODO: do some error handling
        if rc != SUCCESS:
            print(colorize("WARNING: could not successfully update '{}'".format(inst)))


def reset_cluster(name, cluster=None):
    if cluster is None:
        cluster = get_instances(name)
    if not cluster:
        print(colorize("Cluster '{}' does not exist. Did you mean startcluster?".format(name)))
        return
    proxies = {}
    for inst, info in cluster.items():
        proxies[inst] = xmlrpclib.ServerProxy('http://{}:8000'.format(info['ext_ip']))
    print("Resetting cluster '{}'...".format(name))
    for inst, proxy in proxies.items():
        try:
            rc = proxy.reset()
        except socket.error:
            print colorize("WARNING: could not connect to '{}'".format(inst))
            continue  # TODO: do some error handling


if __name__ == '__main__':
    cmd = sys.argv[1]
    if cmd == 'startcluster':
        try:
            cl_name, cl_size = sys.argv[2], int(sys.argv[3])
            if cl_name[0] not in string.ascii_lowercase or '-' in cl_name:
                raise ValueError
            start_cluster(cl_name, cl_size)
        except (IndexError, ValueError):
            print(colorize('Please enter a valid cluster name and size'))
    elif cmd == 'killcluster':
        try:
            cl_name = sys.argv[2]
            kill_cluster(cl_name)
        except IndexError:
            print(colorize('Please enter a valid cluster name'))
    elif cmd == 'resizecluster':
        try:
            cl_name, cl_size = sys.argv[2], int(sys.argv[3])
            resize_cluster(cl_name, cl_size)
        except (IndexError, ValueError):
            print(colorize('Please enter a valid cluster name and size'))
    elif cmd == 'updatecluster':
        try:
            cl_name = sys.argv[2]
            update_cluster(cl_name)
        except IndexError:
            print(colorize('Please enter a valid cluster name'))
    elif cmd == 'resetcluster':
        try:
            cl_name = sys.argv[2]
            reset_cluster(cl_name)
        except IndexError:
            print(colorize('Please enter a valid cluster name'))
    elif cmd == 'listclusters':
        clusters = get_clusters()
        print('{:<28}{:<4}'.format('Cluster', 'Size'))
        print('-------                     ----')
        for cluster, size in clusters.items():
            print('{:<28}{:<4}'.format(cluster, size))
    else:
        print(colorize('Unknown command: {}'.format(cmd)))
        print(colorize('Use one of: startcluster, killcluster, resizecluster, ' + \
                'updatecluster, resetcluster, listclusters'))

