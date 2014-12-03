"""
Server code that gets run on the slave end, to wait for commands to update, reset, or run.
"""

import os
import shlex
import socket
import subprocess
import time
import xmlrpclib

from SimpleXMLRPCServer import SimpleXMLRPCServer
from common import *


IP_ADDR = socket.gethostbyname(socket.gethostname())
RUN_CMD = ['python']
RUN_PROC = None
RUN_ARGS = None
PORTS = None


def update():
    global RUN_PROC, RUN_ARGS, PORTS
    if RUN_PROC is not None:
        if RUN_PROC.poll() is None:
            RUN_PROC.kill()
        RUN_PROC = None
    upd_proc = subprocess.Popen(CONF['update_command'], shell=True)
    upd_proc.wait()
    if upd_proc.returncode != 0:
        RUN_ARGS = None
        PORTS = None
        return ERR_UPD_FAIL
    else:
        if RUN_ARGS is not None:
            RUN_PROC = subprocess.Popen(RUN_CMD + RUN_ARGS + ['--slave=True'], \
                    stdin=subprocess.PIPE, close_fds=True)
            RUN_PROC.stdin.write('{}\n'.format(PORTS))
            RUN_PROC.stdin.close()
        return SUCCESS


def run(script, args, ports):
    global RUN_PROC, RUN_ARGS, PORTS
    if RUN_PROC is not None:
        return RUN_ARGS, PORTS
    else:
        RUN_ARGS = [script] + args
        PORTS = ports
        RUN_PROC = subprocess.Popen(RUN_CMD + RUN_ARGS + ['--slave=True'], \
                stdin=subprocess.PIPE, close_fds=True)
        RUN_PROC.stdin.write('{}\n'.format(PORTS))
        RUN_PROC.stdin.close()
        return SUCCESS


def reset():
    global RUN_PROC, RUN_ARGS, PORTS
    if RUN_PROC is not None:
        if RUN_PROC.poll() is None:
            RUN_PROC.kill()
        RUN_PROC = None
        RUN_ARGS = None
        PORTS = None
    return SUCCESS


with open(os.devnull, 'w') as DEVNULL:
    server = SimpleXMLRPCServer((IP_ADDR, 8000), allow_none=True)
    print('Listening on port 8000...')
    server.register_function(update, 'update')
    server.register_function(run, 'run')
    server.register_function(reset, 'reset')
    server.serve_forever()

