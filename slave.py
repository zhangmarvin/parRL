"""
Main slave parallelization code. Functionality for running the slave loop to receive jobs.
"""

import cPickle
import multiprocessing
import socket
import sys
import zlib
import zmq

from common import *


IP_ADDRESS = CONF['ip_address']
MAPS = set()
APPLIES = set()


def run_slave_loop():
    ports = eval(sys.stdin.read())
    ctx = zmq.Context()
    if CONF['fast']:
        map_rcv = ctx.socket(zmq.PULL)
        map_snd = ctx.socket(zmq.PUSH)
    else:
        map_rcv = ctx.socket(zmq.SUB)
        map_rcv.setsockopt(zmq.SUBSCRIBE, b'')
        map_snd = ctx.socket(zmq.REQ)
        map_snd.setsockopt(zmq.IDENTITY, socket.gethostname())
    map_rcv.connect('tcp://{}:{}'.format(IP_ADDRESS, ports[0]))
    map_snd.connect('tcp://{}:{}'.format(IP_ADDRESS, ports[1]))
    apl_rdy = ctx.socket(zmq.SUB)
    apl_rdy.setsockopt(zmq.SUBSCRIBE, b'')
    apl_wkr = ctx.socket(zmq.REQ)
    apl_wkr.setsockopt(zmq.IDENTITY, socket.gethostname())
    apl_rdy.connect('tcp://{}:{}'.format(IP_ADDRESS, ports[2]))
    apl_wkr.connect('tcp://{}:{}'.format(IP_ADDRESS, ports[3]))
    poller = zmq.Poller()
    poller.register(map_rcv, zmq.POLLIN)
    poller.register(apl_rdy, zmq.POLLIN)
    pool = multiprocessing.Pool()
    try:
        while True:
            socks = dict(poller.poll())
            if map_rcv in socks:
                if CONF['fast']:
                    msg = map_rcv.recv()
                    print len(msg)
                    chunk, func, args_sub = cPickle.loads(msg)
                    print 'received chunk ' + str(chunk)
                    results = pool.map(func, args_sub)
                    try:
                        print len(results)
                        first = results[0]
                        assert all(type(res) == type(first) for res in results)
                        print type(first)
                        print len(first)
                        assert all(type(res) == type(first[0]) for res in first)
                        print type(first[0])
                    except:
                        pass
                    msg = cPickle.dumps((chunk, results))
                    print len(msg)
                    print 'sending results for chunk ' + str(chunk)
                    map_snd.send(msg)
                else:
                    s = cPickle.loads(map_rcv.recv())
                    if s in MAPS:
                        continue
                    MAPS.add(s)
                    map_snd.send(RDY_MSG)
                    while True:
                        msg = map_snd.recv()
                        if msg == END_MSG:
                            break
                        s, chunk, func, args_sub = cPickle.loads(msg)
                        print 'received chunk ' + str(chunk)
                        results = pool.map(func, args_sub)
                        msg_uc = cPickle.dumps((s, chunk, results))
                        print len(msg_uc)
                        msg = zlib.compress(msg_uc, CONF['compress_level'])
                        print len(msg)
                        print 'sending results for chunk ' + str(chunk)
                        map_snd.send(msg)
            if apl_rdy in socks:
                apl_rdy.recv()
                apl_wkr.send(RDY_MSG)
                msg = apl_wkr.recv()
                s, func, args = cPickle.loads(msg)
                if s in APPLIES:
                    continue
                APPLIES.add(s)
                result = func(*args)
                result_str_uc = cPickle.dumps(result)
                print len(result_str_uc)
                result_str = zlib.compress(result_str_uc, CONF['compress_level'])
                print len(result_str)
                apl_wkr.send(result_str)
                msg = apl_wkr.recv()
                if msg != END_MSG:
                    print(colorize('WARNING: expected END got {}'.format(msg), 'red'))
    finally:
        pool.close()
        pool.join()

