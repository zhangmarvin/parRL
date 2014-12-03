"""
Main master parallelization code. Functionality for claiming a cluster, and sending map/apply jobs.
"""

import cPickle
import multiprocessing
import random
import socket
import string
import subprocess
import sys
import time
import xmlrpclib
import zlib
import zmq

from common import *


ALL_INSTS = []
AVAILABLE = []
UNAVAILABLE = []
MAP_NUM = 0
APL_NUM = 0
CTX = zmq.Context()
if CONF['fast']:
    map_snd = CTX.socket(zmq.PUSH)
    map_rcv = CTX.socket(zmq.PULL)
else:
    map_snd = CTX.socket(zmq.PUB)
    map_rcv = CTX.socket(zmq.ROUTER)
    map_rcv.RCVTIMEO = CONF['map_timeout']
apl_rdy = CTX.socket(zmq.PUB)
apl_wkr = CTX.socket(zmq.ROUTER)
apl_wkr.RCVTIMEO = CONF['apply_timeout']


def claim_cluster(name):
    global ALL_INSTS, AVAILABLE, UNAVAILABLE
    cluster = get_instances(name)
    if not cluster:
        raise ValueError(colorize("Cluster '{}' does not exist".format(name)))
    ALL_INSTS = cluster.keys()
    AVAILABLE = ALL_INSTS[:]
    ports = []
    sock = CTX.socket(zmq.REQ)
    for _ in range(4):
        ports.append(sock.bind_to_random_port('tcp://*'))
    sock.close()
    proxies = {}
    for inst, info in cluster.items():
        proxies[inst] = xmlrpclib.ServerProxy('http://{}:8000'.format(info['ext_ip']))
    print("Claiming cluster '{}'...".format(name))
    for inst, proxy in proxies.items():
        try:
            rc = proxy.run(sys.argv[0], sys.argv[1:], ports)
        except socket.error:
            print colorize("WARNING: could not connect to '{}'".format(inst))
            UNAVAILABLE.append(inst)
            AVAILABLE.remove(inst)
            continue  # TODO: do some error handling
        if type(rc) is list:
            if rc[0] == sys.argv:
                ports = rc[1]
                break
            else:
                raise ValueError(colorize("Cluster '{}' is already claimed".format(name)))
        elif rc != SUCCESS:
            UNAVAILABLE.append(inst)
            AVAILABLE.remove(inst)
    map_snd.bind('tcp://*:{}'.format(ports[0]))
    map_rcv.bind('tcp://*:{}'.format(ports[1]))
    apl_rdy.bind('tcp://*:{}'.format(ports[2]))
    apl_wkr.bind('tcp://*:{}'.format(ports[3]))


def fast_parallel_map(func, args_lst):
    result_lst = []
    chunked = []
    chunk_size = 1  # (len(args_lst) // len(ALL_INSTS)) + 1
    for i in range(0, len(args_lst), chunk_size):
        chunk = args_lst[i:i+chunk_size]
        chunked.append(chunk)
    chunks = range(len(chunked))
    time.sleep(20)  #TODO: well this ain't no good
    for ch in chunks:
        print 'sending chunk ' + str(ch)
        map_snd.send(cPickle.dumps((ch, func, chunked[ch])))
    all_chunks = set(chunks)
    received = set()
    while True:
        while True:
            try:
                result_str = map_rcv.recv()
            except zmq.error.Again:
                break
            chunk, result = cPickle.loads(result_str)
            print 'received results for chunk ' + str(chunk)
            received.add(chunk)
            result_lst.extend(result)
            if len(result_lst) == len(args_lst):
                return result_lst
        for ch in all_chunks - received:
            print 'resending chunk {}'.format(ch)
            map_snd.send(cPickle.dumps((ch, func, chunked[ch])))


def reliable_parallel_map(func, args_lst):
    global MAP_NUM
    MAP_NUM += 1
    chunked = []
    chunk_size = 1  # (len(args_lst) // len(ALL_INSTS)) + 1
    for i in range(0, len(args_lst), chunk_size):
        chunked.append(args_lst[i:i+chunk_size])
    num_chunks = len(chunked)
    result_lst = []
    ch = 0
    time.sleep(1)  # TODO: calibrate this for maximum performance
    while True:
        map_snd.send(cPickle.dumps(MAP_NUM))
        try:
            id, _, msg = map_rcv.recv_multipart()
        except zmq.error.Again:
            continue
        if id in UNAVAILABLE:
            map_rcv.send_multipart([id, b'', END_MSG])
            continue
        if msg != RDY_MSG:
            s, chunk, results = cPickle.loads(zlib.decompress(msg))
            if s != MAP_NUM:
                map_rcv.send_multipart([id, b'', END_MSG])
                continue
            if chunked[chunk] is not None:
                print 'received results for chunk ' + str(chunk)
                chunked[chunk] = None
                num_chunks -= 1
                result_lst.extend(results)
                if num_chunks == 0:
                    break
        while chunked[ch] is None:
            ch = (ch + 1) % len(chunked)
        msg = cPickle.dumps((MAP_NUM, ch, func, chunked[ch]))
        print 'sending chunk ' + str(ch)
        map_rcv.send_multipart([id, b'', msg])
        ch = (ch + 1) % len(chunked)
    for id in AVAILABLE:
        map_rcv.send_multipart([id, b'', END_MSG])
    return result_lst

parallel_map = fast_parallel_map if CONF['fast'] else reliable_parallel_map


def apply_on_all_insts(func, args):
    global AVAILABLE, UNAVAILABLE, APL_NUM
    APL_NUM += 1
    wf(colorize('applying on slaves... ', 'yellow'))
    insts_left = ALL_INSTS[:]
    results = []
    func_call = cPickle.dumps((APL_NUM, func, args))
    for _ in range(CONF['apply_tries']):
        apl_rdy.send(b'')
        while True:
            try:
                id, _, msg = apl_wkr.recv_multipart()
            except zmq.error.Again:
                break
            if msg == RDY_MSG:
                apl_wkr.send_multipart([id, b'', func_call])
            else:
                result = cPickle.loads(zlib.decompress(msg))
                results.append(result)
                apl_wkr.send_multipart([id, b'', END_MSG])
                insts_left.remove(id)
                if not insts_left:
                    UNAVAILABLE = []
                    AVAILABLE = ALL_INSTS[:]
                    print(colorize('done', 'green'))
                    return results
        if not CONF['fast']:
            for id in insts_left:
                map_rcv.send_multipart([id, b'', END_MSG])
    UNAVAILABLE = insts_left
    AVAILABLE = list(set(ALL_INSTS) - set(UNAVAILABLE))
    if not AVAILABLE:
        raise ValueError('No instances available')
    print(colorize('done', 'green'))
    print(colorize('UNAVAILABLE: {}'.format(UNAVAILABLE)))
    return results

