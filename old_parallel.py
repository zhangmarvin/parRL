"""
This file is around for historical purposes more than anything else. Don't use it!
"""

import cPickle
import multiprocessing
import socket
import subprocess
import sys
import xmlrpclib
import zmq

from common import *


IP_ADDRESS = '169.229.222.223'  # or '168' for robocop
ALL_INSTS = []
UNAVAILABLE = []
RDY_MSG = b'READY'
END_MSG = b'END'
CTX = zmq.Context()
PORTS = []


def claim_cluster(name):
    global PORTS, ALL_INSTS, UNAVAILABLE
    cluster = get_instances(name)
    ALL_INSTS = cluster.keys()
    sock = CTX.socket(zmq.REQ)
    for _ in range(4):
        PORTS.append(sock.bind_to_random_port('tcp://*'))
    sock.close()
    if not cluster:
        raise ValueError(colorize("Cluster '{}' does not exist".format(name)))
    proxies = {inst: xmlrpclib.ServerProxy('http://{}:8000'.format(info['ext_ip'])) \
            for inst, info in cluster.items()}
    print("Claiming cluster '{}'...".format(name))
    for inst, proxy in proxies.items():
        rc = proxy.run(sys.argv[0], sys.argv[1:], PORTS)
        if type(rc) is list:
            if rc[0] == sys.argv:
                PORTS = rc[1]
            else:
                raise ValueError(colorize("Cluster '{}' is already claimed".format(name)))
        if rc != SUCCESS:
            UNAVAILABLE.append(inst)


def run_slave_loop(ports):
    map_receiver = CTX.socket(zmq.PULL)
    map_receiver.connect('tcp://{}:{}'.format(IP_ADDRESS, ports[0]))
    map_sender = CTX.socket(zmq.PUSH)
    map_sender.connect('tcp://{}:{}'.format(IP_ADDRESS, ports[1]))
    apply_receiver = CTX.socket(zmq.SUB)
    apply_receiver.connect('tcp://{}:{}'.format(IP_ADDRESS, ports[2]))
    apply_receiver.setsockopt(zmq.SUBSCRIBE, b'')
    apply_sender = CTX.socket(zmq.PUSH)
    apply_sender.connect('tcp://{}:{}'.format(IP_ADDRESS, ports[3]))
    poller = zmq.Poller()
    poller.register(map_receiver, zmq.POLLIN)
    poller.register(apply_receiver, zmq.POLLIN)
    while True:
        socks = dict(poller.poll(1000))
        if map_receiver in socks:
            pool = multiprocessing.Pool()
            func_str, args_str = map_receiver.recv_multipart()
            func = cPickle.loads(func_str)
            args_sub = cPickle.loads(args_str)
            results = pool.map(func, args_sub)
            try:
                result_str = cPickle.dumps(results)
            except cPickle.PicklingError as e:
                result_str = cPickle.dumps(e)
            map_sender.send(result_str)
            pool.close()
            pool.join()
        if apply_receiver in socks:
            func_str, args_str = apply_receiver.recv_multipart()
            func = cPickle.loads(func_str)
            args = cPickle.loads(args_str)
            try:
                result = func(*args)
            except Exception as e:
                result = e
            try:
                result_str = cPickle.dumps(result)
            except cPickle.PicklingError as e:
                result_str = cPickle.dumps(e)
            apply_sender.send(result_str)


def parallel_map(func, args_lst):
    map_sender = CTX.socket(zmq.PUSH)
    map_sender.bind('tcp://*:{}'.format(PORTS[0]))
    map_receiver = CTX.socket(zmq.PULL)
    map_receiver.bind('tcp://*:{}'.format(PORTS[1]))
    result_lst = []
    try:
        func_str = cPickle.dumps(func)
    except cPickle.PicklingError:
        print(colorize('could not pickle {}, quitting'.format(func), 'red'))
        return
    chunk_size = (len(args_lst) // len(ALL_INSTS)) + 1
    for i in range(0, len(args_lst), chunk_size/2):
        args_sub = args_lst[i:i+chunk_size/2]
        try:
            args_str = cPickle.dumps(args_sub)
        except cPickle.PicklingError:
            print(colorize('could not pickle {}, quitting'.format(args_sub), 'red'))
            return
        map_sender.send_multipart([func_str, args_str])
    while True:
        try:
            result_str = map_receiver.recv(flags=zmq.NOBLOCK)
        except zmq.error.Again:
            continue
        result = cPickle.loads(result_str)
        if isinstance(result, Exception):
            print(colorize('exception occurred while mapping', 'red'))
        result_lst.extend(result)
        if len(result_lst) == len(args_lst):
            break
    map_sender.close()
    map_receiver.close()
    return result_lst


def apply_on_all_insts(func, args):
    apply_sender = CTX.socket(zmq.PUB)
    apply_sender.bind('tcp://*:{}'.format(PORTS[2]))
    apply_receiver = CTX.socket(zmq.PULL)
    apply_receiver.bind('tcp://*:{}'.format(PORTS[3]))
    apply_receiver.RCVTIMEO = 3000
    wf(colorize('applying on slaves', 'yellow') + ':\t')
    try:
        func_str = cPickle.dumps(func)
        args_str = cPickle.dumps(args)
    except cPickle.PickingError:
        print(colorize('could not pickle {} or {}, quitting'.format(func, args), 'red'))
    num_tries = 0
    while True:
        apply_sender.send_multipart([func_str, args_str])
        result_lst = []
        while True:
            try:
                result_str = apply_receiver.recv()
                result = cPickle.loads(result_str)
            except zmq.error.Again:
                num_tries += 1
                wf(colorize('{} '.format(num_tries), 'red'))
                break
            if isinstance(result, Exception):
                print(colorize('exception occurred while applying', 'red'))
            result_lst.append(result)
            if len(result_lst) == len(ALL_INSTS):
                print(colorize(str(num_tries + 1), 'green'))
                apply_sender.close()
                apply_receiver.close()
                return result_lst

