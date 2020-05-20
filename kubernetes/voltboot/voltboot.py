#!/usr/bin/env python
# -*-mode: python-*-

# This file is part of VoltDB.
# Copyright (C) 2020 VoltDB Inc.

# VoltDB Kubernetes node startup controller
# for use with the VoltDB operator for Kubernetes
#
# Environment variables understood by voltboot:
#
# +-----Variable name----------------+----Default value------+
# | VOLTDB_INIT_VOLUME               | /etc/voltdb           |
# | VOLTDB_K8S_ADAPTER_ADMIN_PORT    | 8080                  |
# | VOLTDB_K8S_ADAPTER_FQHOSTNAME    | from socket.getfqdn() |
# | VOLTDB_K8S_ADAPTER_INTERNAL_PORT | 3021                  |
# | VOLTDB_K8S_ADAPTER_PVVOLTDBROOT  | /voltdbroot           |
# | VOLTDB_K8S_ADAPTER_STATUS_PORT   | 11780                 |
# | VOLTDB_K8S_ADAPTER_VOLTBOOT_PORT | 11235                 |
# | VOLTDB_CONNECTION_HOST           |                       | // TODO: Dave
# +----------------------------------+-----------------------+

import logging
import os
import re
import shlex
import socket
import subprocess
import sys
import random

from flask import Flask, request
from werkzeug.exceptions import BadRequest, InternalServerError
from tempfile import mkstemp
from threading import Thread, Lock
from time import time, sleep, strftime, gmtime, localtime, timezone
from traceback import format_exc

# Mount point for persistent volume 'voltdbroot'
PV_MOUNTPT = os.getenv('VOLTDB_K8S_ADAPTER_PVVOLTDBROOT', '/voltdbroot')

# Hardwired name of voltdbroot directory (which is the VoltDB default)
VOLTDBROOT = 'voltdbroot'

# Assets directory: mounted by config maps
ASSETS_DIR = os.path.join(os.getenv('VOLTDB_INIT_VOLUME', '/etc/voltdb'))

# Ports used by this program for 'node up?' testing
VOLTDB_INTERNAL_INTERFACE = int(os.getenv('VOLTDB_K8S_ADAPTER_INTERNAL_PORT', 3021))
VOLTDB_HTTP_PORT = int(os.getenv('VOLTDB_K8S_ADAPTER_ADMIN_PORT', 8080))
VOLTDB_STATUS_PORT = int(os.getenv('VOLTDB_K8S_ADAPTER_STATUS_PORT', 11780))
VOLTBOOT_PORT = int(os.getenv('VOLTDB_K8S_ADAPTER_VOLTBOOT_PORT', 11235))
                                                                // TODO: Dave
# URL for --host instead of hunting for it                      // TODO: Dave
VOLTDB_CONNECTION_HOST = os.getenv('VOLTDB_CONNECTION_HOST')    // TODO: Dave

# Time spent waiting to see if voltdb has really started (distinguishes
# 'start failed' from 'crashed')
START_WAIT = 2

# Time spent waiting for DNS to have our own name (avoids waiting forever
# if there is a bug somewhere)
DNS_WAIT = 10

# Exceptions
class ForbiddenArgException(Exception):
    pass
class VoltInitException(Exception):
    pass
class VoltStartException(Exception):
    pass

####
# Global data.
# Most accesses are from synchronous server code.
# The exception is STATE, which may be updated from
# a background monitoring thread, thus the lock.

HOSTINFO = None
FQHOSTNAME = None
SERVER = None
WORKING_DIR = None
VOLTBOOT_DIR = None
RESTART_ARGS = None
RESTART_NAME = None
STARTED_NAME = None
STATE = None

ST_UNINIT    = 'uninitialized'
ST_STARTING  = 'starting'
ST_STARTFAIL = 'start-failed'
ST_RUNNING   = 'running'
ST_STOPPED   = 'stopped'
ST_CRASHED   = 'crashed'

state_lock = Lock()

def set_state(state):
    global STATE
    state_lock.acquire()
    try:
        if STATE != state:
            STATE = state
            logging.info("VoltDB state is now '%s'", state)
    finally:
        state_lock.release()

def set_state_if(old_state, new_state):
    global STATE
    state_lock.acquire()
    try:
        if STATE == old_state:
            STATE = new_state
            logging.info("VoltDB state is now '%s'", new_state)
    finally:
        state_lock.release()

####
# Main program code: a simple REST server
# that drives all activity. Loops forever.

def main():
    global HOSTINFO, FQHOSTNAME, SERVER, WORKING_DIR, VOLTBOOT_DIR, \
        RESTART_ARGS, RESTART_NAME, STARTED_NAME, STATE

    # See if the persistent storage mount exists
    if not os.path.exists(PV_MOUNTPT):
        logging.error("Persistent volume '%s' is not mounted", PV_MOUNTPT)
        sys.exit(1)

    # Parse our own hostname, of known format, to get useful data about cluster
    FQHOSTNAME = get_fqhostname()
    HOSTINFO = split_fqhostname(FQHOSTNAME)
    if HOSTINFO is None:
        logging.error("Hostname parse error, unexpected form: '%s'", FQHOSTNAME)
        sys.exit(1)
    ssname, pod_ordinal, my_hostname, domain = HOSTINFO
    if type(pod_ordinal) != type(0):
        logging.error("Hostname parse error, expected numeric pod ordinal: '%s'", pod_ordinal)
        sys.exit(1)

    # Set up logger, log startup banner
    setup_logging(ssname)
    logging.info("==== VoltDB startup controller for Kubernetes ====")
    logging.info("GMT is: " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) +
                 " LOCALTIME is: " + strftime("%Y-%m-%d %H:%M:%S", localtime()) +
                 " TIMEZONE OFFSET is: " + str(timezone))
    if logging.getLogger().isEnabledFor(logging.DEBUG):
        logging.debug("Environment:")
        for k, v in os.environ.items():
            logging.debug("  %s = %s", k, v)
        logging.debug("Current dir: %s", os.getcwd())
    logging.info("Host: %s", FQHOSTNAME)
    logging.info("Pod: %s-%s", ssname, pod_ordinal)

    # The persistent volume (PV) is mounted on to a directory we know as PV_MOUNTPT.
    # In that directory, i.e., in the root of the PV, we will create a container-
    # specific directory with an arbitrary name (see initialization below). This
    # directory is the working directory for the rest of the present program; it
    # is signficant because it (the working directory) is the directory in which
    # 'voltdb init' creates the voltdbroot directory, and in which 'voltdb start'
    # expects to find the voltdb directory.
    #
    # Since the name of the working directory is arbitrary, we set up a symbolic
    # link in the root of the PV, named after the stateful set, and pointing to
    # the working directory.
    #
    # Voltboot gets its own directory under the working directory. This is used
    # to store restart command info and anything else specific to voltboot.
    WORKING_DIR = os.path.join(PV_MOUNTPT, ssname)
    VOLTBOOT_DIR = os.path.join(WORKING_DIR, 'voltboot')
    if os.path.exists(os.path.join(WORKING_DIR, VOLTDBROOT, '.initialized')):
        os.chdir(WORKING_DIR)
        logging.info("Working directory is: %s", WORKING_DIR)
        (RESTART_ARGS, RESTART_NAME) = load_restart_args()
        if RESTART_ARGS:
            auto_restart(RESTART_ARGS, RESTART_NAME)
        else:
            set_state(ST_STOPPED)
    else:
        set_state(ST_UNINIT)

    # Set up web server. All error responses are reported by
    # an HTTP exception.  We intercept the exception and
    # turn it into a string-and-status; this prevents the
    # default handling that returns (ugh) an HTML page.
    class VoltbootServer(Flask):
        def handle_http_exception(env, ex):
            logging.error("%s (%s)", ex.description, ex.code)
            return ex.description, ex.code
        def handle_exception(env, ex):
            logging.error("Unhandled: %s", ex.args[0])
            return ex.args[0], 500
    SERVER = VoltbootServer('voltboot')

    # Endpoint processing follows. The pattern is that
    # we handle the request parsing here, then call a
    # processing routine that has approriate exeception
    # handlers for all expected cases.
    @SERVER.route('/status', methods=['GET'])
    def get_status():
        state = STATE # read once, we hope
        logging.debug("GET /status in state '%s'", state)
        resp = status_response(state, STARTED_NAME, RESTART_NAME)
        if state == ST_CRASHED: # need liveness check to fail
            return (resp, 424)  # "failed dependency"
        else:
            return resp

    @SERVER.route('/start', methods=['POST'])
    def post_start():
        logging.debug("POST /start in state '%s'", STATE)
        req = request.get_json(force=True, cache=False) # may raise 400 exception
        start_cmd = command_parts(req, 'startCommand', True)
        restart_cmd = command_parts(req, 'restartCommand', False)
        init_and_start(start_cmd, restart_cmd)
        return status_response(STATE, STARTED_NAME, RESTART_NAME)

    @SERVER.route('/restart-command', methods=['POST'])
    def post_restart_cmd():
        logging.debug("POST /restart-command")
        req = request.get_json(force=True, cache=False) # may raise 400 exception
        restart_cmd = command_parts(req, 'restartCommand', True)
        update_restart_cmd(restart_cmd[0], restart_cmd[1])
        return restart_response(RESTART_ARGS, RESTART_NAME)

    @SERVER.route('/restart-command', methods=['DELETE'])
    def delete_restart_cmd():
        logging.debug("DELETE /restart-command")
        prev_args = RESTART_ARGS
        prev_name = RESTART_NAME
        update_restart_cmd(None, None)
        return restart_response(prev_args, prev_name)

    @SERVER.route('/restart-command', methods=['GET'])
    def get_restart_cmd():
        logging.debug("GET /restart-command")
        return restart_response(RESTART_ARGS, RESTART_NAME)

    # And start serving
    logging.info("Ready for commands ...")
    cli = sys.modules['flask.cli']
    cli.show_server_banner = lambda *x: None
    SERVER.run(host='0.0.0.0', port=VOLTBOOT_PORT, debug=False, load_dotenv=False)

def status_response(state, started_name, restart_name):
    resp = { "status": state }
    if started_name:
        resp.update({ "startedName": started_name })
    if restart_name:
        resp.update({ "restartName": restart_name })
    return resp

def command_parts(req, name, mandatory):
    obj = {}
    if name in req:
        obj = req[name]
    elif mandatory:
        raise BadRequest(name + ' object is required')
    if type(obj) != type({}):
        raise BadRequest(name + ' must be a JSON object {...}')
    args = normalize_args(_json_array(obj, 'args'))
    name = _json_string(obj, 'name')
    return (args, name)

def _json_array(obj, name):
    arr = obj[name] if name in obj else []
    if type(arr) != type([]):
        raise BadRequest(name + ' must be a JSON array [...]')
    return arr

def _json_string(obj, name):
    txt = obj[name] if name in obj else ''
    if type(txt) != type('') and type(txt) != type(u''):
        raise BadRequest(name + ' must be a JSON string "..."')
    return txt

def init_and_start(start_cmd, restart_cmd):
    try:
        start_args = start_cmd[0]
        start_name = start_cmd[1]
        restart_args = restart_cmd[0]
        restart_name = restart_cmd[1]
        check_forbidden_args(start_args)
        check_forbidden_args(restart_args)
        if STATE == ST_RUNNING or STATE == ST_STARTING:
            raise BadRequest("Start not valid in state '%s'" % STATE)
        if STATE == ST_UNINIT:
            init_voltdb()
        set_state(ST_STARTING)
        start_voltdb(start_args, start_name)
        save_restart_cmd(restart_args, restart_name)
    except ForbiddenArgException as e:
        raise BadRequest(e.args[0])
    except VoltInitException as e:
        raise InternalServerError(e.args[0])
    except VoltStartException as e:
        set_state_if(ST_STARTING, ST_STARTFAIL)
        raise InternalServerError(e.args[0])
    except BadRequest:
        raise
    except Exception as e:
        logging.error("Unexpected exception: %s", e)
        set_state_if(ST_STARTING, ST_STARTFAIL)
        raise InternalServerError("Unexpected exception", e.args[0], e)

def auto_restart(restart_args, restart_name):
    try:
        set_state(ST_STARTING)
        start_voltdb(restart_args, restart_name)
    except VoltStartException as e:
        set_state_if(ST_STARTING, ST_STARTFAIL)
        raise InternalServerError(e.args[0])
    except Exception as e:
        logging.error("Unexpected exception: %s", e)
        set_state_if(ST_STARTING, ST_STARTFAIL)
        raise InternalServerError("Unexpected exception", e.args[0], e)

####
# VoltDB initialization

def init_voltdb(force=False):
    logging.info("Initializing a new VoltDB database in '%s'", WORKING_DIR)
    ssname, pod_ordinal, my_hostname, domain = HOSTINFO

    # Remove any old symlink
    os.chdir(PV_MOUNTPT)
    try:
        os.unlink(ssname)
    except:
        pass

    # Create working dir with arbitrary but unique name
    workdir_unique = 'VDBR-' + str(pod_ordinal) + '-' + str(int(time()*1e6)) + '.' + domain
    os.mkdir(workdir_unique)

    # Add symlink to PV root
    os.symlink(workdir_unique, ssname)

    # Construct voltdb command string
    os.chdir(workdir_unique)
    cmd = ['voltdb', 'init']

    if force:
        cmd.append('--force')

    if os.path.isdir(ASSETS_DIR):
        deployment_file = os.path.join(ASSETS_DIR, 'deployment')
        if os.path.isfile(deployment_file):
            cmd.append('--config')
            cmd.append(deployment_file)
        classes_dir = os.path.join(ASSETS_DIR, 'classes')
        l = get_files_list(classes_dir)
        if l is not None:
            cmd.append('--classes')
            cmd.append(','.join(l))
        schema_dir = os.path.join(ASSETS_DIR, 'schema')
        l = get_files_list(schema_dir)
        if l is not None:
            cmd.append('--schema')
            cmd.append(','.join(l))

    # Run voltdb and wait for it to finish (does local
    # work only, so wait should not be too long)
    logging.info("Executing VoltDB init command: %s", cmd)
    logging.info(" in working directory: %s", os.getcwd())

    sys.stdout.flush()
    sys.stderr.flush()

    try:
        sp = subprocess.Popen(cmd, shell=False)
        sp.wait()
    except Exception as e:
        raise VoltInitException("Failed to run 'voltdb init' command: %s" % e)

    if sp.returncode != 0:
        raise VoltInitException("Failed to initialize VoltDB database in '%s'" % WORKING_DIR)

    marker = os.path.join(WORKING_DIR, VOLTDBROOT, '.initialized')
    if not os.path.exists(marker):
        raise VoltInitException("VoltDB initialization succeeded but marker file '%s' was not created" % marker)

    logging.info("Initialization of new VoltDB database is complete")
    setup_logging(ssname)  # logging changes to use new directory
    os.chdir(WORKING_DIR)
    logging.info("Working directory is now: %s", WORKING_DIR)

    # Create scratch directory for voltboot
    os.mkdir(VOLTBOOT_DIR)

def get_files_list(dir):
    # skip files starting with .., such as ..data, that k8s puts in configmaps
    files = [f for f in os.listdir(dir) if not f.startswith('..')]
    if len(files) > 1:
        plf = os.path.join(dir, '.loadorder')
        if os.path.exists(plf):
            with open(plf, 'r') as f:
                fl = f.readline().strip().split(',')
            fqpl = map(lambda x: os.path.join(dir, x), fl)
        else:
            fqpl = [ dir + '/*', ]
        return fqpl
    elif len(files) == 1:
        return [ os.path.join(dir, files[0]) ]
    return None

###
# Start VOLTDB running in subprocess
# State is set to STARTING prior to entry
# and will be RUNNING or STARTFAIL on return

def start_voltdb(args, name):
    global STARTED_NAME
    STARTED_NAME = name

    # Find hosts in our cluster
    ssname, pod_ordinal, my_hostname, domain = HOSTINFO
    connect_hosts = discover_pods(FQHOSTNAME, domain, ssname, pod_ordinal)

    # Override some command arguments
    if os.path.isdir(ASSETS_DIR):
        license_file = os.path.join(ASSETS_DIR, 'license') # FIXME license handling moves to init soon
        if os.path.isfile(license_file): # TODO: this causes problems somewhere... dave to figure it out
            add_or_replace_arg(args, '-l,--license', license_file)
    add_or_replace_arg(args, '-H,--host', random.choice(connect_hosts))
    add_or_replace_arg(args, '--status', str(VOLTDB_STATUS_PORT))

    # In voltdbroot/config/path.properties the paths may contain
    # references to voltdbroot. Ensure they always use the correct
    # symlink.
    propdir = os.path.join(VOLTDBROOT, 'config')
    propfile = os.path.join(VOLTDBROOT, 'config', 'path.properties')
    res = '=(.*)/.+?\.' + domain.replace('.','\.') +'/'
    cre = re.compile(res, flags=re.MULTILINE)
    with open(propfile, 'r') as f:
        lines = f.read()
        if len(lines) == 0:
            raise VoltStartException("File '%s' is empty" % propfile)
        lines = cre.sub('=\g<1>/'+ssname+'/', lines)
        tfd, tmpfilepath = mkstemp(dir=propdir)
        with os.fdopen(tfd, 'w') as f2:
            f2.write(lines)
    os.rename(tmpfilepath, propfile)

    # Build the voltdb start command line
    cmd = ['voltdb', 'start']
    cmd.extend(args)
    logging.info("Executing VoltDB start command: %s", cmd)
    logging.info(" in working directory: %s", os.getcwd())

    # Flush so we see our output in k8s logs
    sys.stdout.flush()
    sys.stderr.flush()

    # Start voltdb in subprocess (voltdb cli eventually
    # execs the actual VoltDB program in a JVM)
    try:
        sp = subprocess.Popen(cmd, shell=False)
    except Exception as e:
        raise VoltStartException("Failed to run 'voltdb start' command: %s" % e)

    # Wait a little to see if voltdb starts ok
    t0 = time()
    while True:
        if sp.poll() is not None:
            set_state(ST_STARTFAIL)
            raise VoltStartException("VoltDB was started and immediately terminated")
        if time()-t0 >= START_WAIT:
            break
        sleep(0.5)

    logging.info("VoltDB started as process id %d", sp.pid)
    set_state(ST_RUNNING)

    # Now run a monitoring thread. This may asynchronously
    # change the state to show that VoltDB has terminated.
    def voltdb_monitor(proc):
        logging.debug("Monitoring thread started")
        excode = None
        try:
            excode = proc.wait()
        except Exception as e:
            logging.error("Monitoring thread error: %s", e)
            excode = -1
        if excode == 0:
            logging.info("VoltDB process terminated normally")
            set_state_if(ST_RUNNING, ST_STOPPED)
        else:
            logging.error("VoltDB process terminated abnormally")
            set_state_if(ST_RUNNING, ST_CRASHED)
        logging.debug("Monitoring thread terminated")

    th = Thread(target=voltdb_monitor, args=(sp,))
    th.daemon = True
    th.start()

# Find nodes which have the mesh port open and which respond to HTTP traffic
# Nodes may be "published before they are ready to receive traffic"
def discover_pods(fqhostname, domain, ssname, pod_ordinal):
    if VOLTDB_CONNECTION_HOST is not None:               // TODO: Dave
        cluster_pods_up = [VOLTDB_CONNECTION_HOST]       // TODO: Dave
        return cluster_pods_up                           // TODO: Dave
                                                         // TODO: Dave
    tstart = time()
    tlog = 0
    seen_own_name = False
    logging.info("This is %s-%s", ssname, pod_ordinal)
    while True:
        cluster_pods = query_dns_srv(domain)
        if fqhostname in cluster_pods: # remove ourself
            cluster_pods.remove(fqhostname)
            if not seen_own_name:
                logging.info("Own name seen in DNS results: %s", fqhostname)
                seen_own_name = True
        cluster_pods_responding_mesh = []
        cluster_pods_up = []

        # Test connectivity to all named pods
        for host in cluster_pods:
            logging.info("Testing connection to '%s'", host)
            if try_to_connect(host, VOLTDB_INTERNAL_INTERFACE):
                cluster_pods_responding_mesh.append(host)
                # We may have found a running node, try the HTTP API
                if try_to_connect(host, VOLTDB_HTTP_PORT):
                    cluster_pods_up.append(host)
        logging.debug("Database nodes up: %s", cluster_pods_up)
        logging.debug("Mesh ports responding: %s", cluster_pods_responding_mesh)

        # If the database is up use all that are available
        if len(cluster_pods_up) > 0:
            return cluster_pods_up

        # If the database is down
        # - forming initial mesh we direct the connection request to host0
        # - bring up pods in an orderly fashion, one at a time
        mesh_count =  len(cluster_pods_responding_mesh)
        if mesh_count >= pod_ordinal:
            logging.debug("Mesh count %d >= pod ordinal %d", mesh_count, pod_ordinal)
            return [ ssname + '-0.' + domain ]
            break

        # If we haven't seen our own name after a few seconds,
        # surely something is broken.
        tnow = time()
        if not seen_own_name and tnow > tstart + DNS_WAIT:
            raise VoltStartException("DNS results don't contain our name: %s" % fqhostname)

        # Log lack of progress, but infrequently
        if tnow > tlog + 30:
            logging.info("Waiting for mesh to form")
            tlog = tnow

        sleep(1)

# DNS lookup. Voltdb stateful set pods are registered on startup not on readiness.
# SRV gives us records for each node in the cluster domain like
#    _service._proto.name. TTL class SRV priority weight port target.
# Returns a list of fq hostnames of pods in the service domain
def query_dns_srv(query):
    m_list = []
    try:
        logging.debug("DNS lookup: %s", query)
        cmd = "nslookup -type=SRV %s | awk '/^%s/ {print $NF}'" % ((query,)*2)
        answers = subprocess.check_output(cmd, shell=True)
        logging.debug("Answers: %s", answers)
    except Exception as e:
        logging.error("DNS query error: %s", e)
        return m_list
    for rdata in answers.split('\n'):
        if len(rdata):
            m_list.append(rdata.split(' ')[-1][:-1])  # drop the trailing '.'
    logging.debug("Results: %s", m_list)
    return sorted(m_list)

def try_to_connect(host, port):
    s = socket.socket()
    try:
        logging.debug("Trying to connect to '%s:%d'", host, port )
        s.connect((host, port))
        logging.debug("Connected")
        return True
    except Exception as e:
        logging.debug(str(e))
        return False
    finally:
        s.close()

####
# Restart command utilities

def update_restart_cmd(arg_list, name):
    if STATE == ST_UNINIT:
        raise BadRequest("Request not valid in '%s' state" % STATE)
    try:
        if arg_list:
            check_forbidden_args(arg_list)
        save_restart_cmd(arg_list, name)
    except ForbiddenArgException as e:
        raise BadRequest(e.args[0])
    except BadRequest:
        raise
    except Exception as e:
        logging.error("Unexpected exception: %s", e)
        raise InternalServerError("Unexpected exception", e.args[0], e)

def save_restart_cmd(arg_list, name):
    global RESTART_ARGS, RESTART_NAME
    if not arg_list and not name:
        RESTART_ARGS = None
        RESTART_NAME = None
        _remove_restart_file()
    elif arg_list != RESTART_ARGS or name != RESTART_NAME:
        RESTART_ARGS = arg_list
        RESTART_NAME = name
        logging.info("Restart command set: name '%s', args %s", name, arg_list)
        _write_restart_file(arg_list, name)

RESTART_FILE_MAGIC='##VOLT 1'

def _write_line(f, t):
    if t is None:
        f.write('\n')
    else:
        f.write(t + '\n')

def _write_restart_file(arg_list, name):
    path = os.path.join(VOLTBOOT_DIR, 'restart-command')
    try:
        with open(path, 'w') as f:
            _write_line(f, RESTART_FILE_MAGIC)
            _write_line(f, name)
            if arg_list:
                for a in arg_list:
                    _write_line(f, a)
    except EnvironmentError as e:
        logging.warning("Failed to write to %s: %s", path, e)

def _remove_restart_file():
    path = os.path.join(VOLTBOOT_DIR, 'restart-command')
    try:
        os.unlink(path)
    except EnvironmentError as e:
        if e.errno != 2: # 'file not found' is ok
            logging.warning("Failed to remove %s: %s", path, e)

def _read_lines(f):
    lines = f.readlines();
    return [ line.strip() for line in lines ]

def load_restart_args():
    path = os.path.join(VOLTBOOT_DIR, 'restart-command')
    data = None
    name = None
    try:
        with open(path, 'r') as f:
            lines = _read_lines(f)
        if lines[0] == RESTART_FILE_MAGIC:
            name = lines[1]
            data = [ line for line in lines[2:] ]
            logging.info("Loaded restart command: name '%s', args %s", name, data)
    except EnvironmentError as e:
        if e.errno == 2: # 'file not found' is ok
            logging.debug("Not found: %s", path)
        else:
            logging.warning("Failed to load %s: %s", path, e)
    return (data, name)

def restart_response(arg_list, name):
    cmd = {}
    if arg_list:
        cmd.update({ "args": arg_list })
    if name:
        cmd.update({ "name": name })
    return { "restartCommand": cmd } if cmd else {}

####
# Command-line parsing utilities

# Normalize args
# - if yaml file contains $(FOO) and configset does not define FOO then the args
#   will contain a literal "$(FOO)" which we do not want.
# - some of our "args" might be environment strings of args; if so break them
#   up for the shell
def normalize_args(args):
    logging.debug("normalize_args in: %s", args)
    nargs = []
    omit = re.compile(R'^\$\(\w+\)$')
    for a in args:
        if omit.match(a):
            logging.info("Omitting unsubstituted variable: %s", a)
        elif ' ' in a:
            nargs.extend(str_to_arg_list(a))
        else:
            nargs.append(a)
    logging.debug("normalize_args out: %s", args)
    return nargs

# Replaces "foo bar=mumble" by [foo, bar, mumble]
def str_to_arg_list(text):
    logging.debug("str_to_arg_list in: %s", text)
    al = []
    for a in shlex.split(text.strip("'\"")):
        if '=' in a:
            al.extend(a.split('=', 1))
        else:
            al.append(a)
    logging.debug("str_to_arg_list out: %s", al)
    return al

# Filter out arguments that are valid for voltdb start
# but which are not allowed here
FORBIDDEN_ARGS = ['--version',
                  '-h', '--help',
                  '-D', '--dir',
                  '-f', '--force',
                  '-B', '--background',
                  '-r', '--replica']

def check_forbidden_args(args):
    logging.debug("check_forbidden_args: %s", args)
    bad = []
    for a in args:
        if a in FORBIDDEN_ARGS:
            bad.append(a)
    if bad:
        tmp = ('' if len(bad) == 1 else 's', ', '.join(bad))
        raise ForbiddenArgException("Unsupported argument%s: %s" % tmp)

def add_or_replace_arg(args, option, value):
    # Option is comma-separated list of option formats to be treated equally,
    # e.g. '-L,--license'. We assume that only one of the option formats is present
    options = option.split(',')
    for op in options:
        ix = find_arg_index(args, op)
        if ix is not None:
            break
    if ix is None: # add
        args.append(op)
        args.append(value)
    else: # replace
        args[ix] = value

def find_arg_index(args, arg):
    for i in range(0, len(args)):
        if args[i] == arg:
            return i+1
    return None

####
# Hostname utilities
# Hostname is like  pear-1.pear.default.svc.cluster.local
# which is ssname-PODORDINAL.FQDOMAIN

def get_fqhostname():
    return os.getenv('VOLTDB_K8S_ADAPTER_FQHOSTNAME', socket.getfqdn())

def split_fqhostname(fqdn):
    try:
        hostname, domain = fqdn.split('.', 1)
        ssp = hostname.split('-')
        pod = ssp[-1]
        if pod.isdigit(): pod = int(pod)
        hn = ('-'.join(ssp[0:-1]), pod, hostname, domain)
    except:
        return None
    return hn # returns (ss-name, pod-ordinal, hostname, domain)

####
# Logging setup.
# We set up console logging to stderr, where it can be found
# by 'kubectl logs PODNAME', and to the same log file that
# VoltDB itself uses. The latter is not available before we
# have run 'voltdb init' for the first time.

def setup_logging(ssname):
    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)
    logger.propagate = True
    log_format = '%(asctime)s %(levelname)-8s %(filename)14s:%(lineno)-6d %(message)s'
    formatter = logging.Formatter(log_format)
    loglevel = get_loglevel('VOLTDB_K8S_ADAPTER_LOG_LEVEL', logging.INFO)
    logger.handlers = []

    # Console
    console = logging.StreamHandler()
    console.setLevel(loglevel)
    console.setFormatter(formatter)
    logger.addHandler(console)

    logto = 'console'
    # And the volt log file if possible
    volt_log = find_volt_log(ssname)
    if volt_log:
        file = logging.FileHandler(volt_log, 'a')
        file.setLevel(loglevel)
        file.setFormatter(formatter)
        logger.addHandler(file)
        logto += ' and ' + volt_log

    # Note loggng destination
    logging.info("Logging to %s", logto)

_logmap = { 'DEBUG':logging.DEBUG, 'INFO':logging.INFO, 'WARNING':logging.WARNING, 'ERROR':logging.ERROR }

def get_loglevel(envar, deflt):
    logstr = os.getenv(envar)
    if logstr is not None:
        logstr = logstr.upper()
        if logstr in _logmap:
            return _logmap[logstr]
    return deflt

def find_volt_log(ssname):
    volt_log = os.path.abspath(os.path.join(PV_MOUNTPT, ssname, VOLTDBROOT, 'log', 'volt.log'))
    if os.path.exists(volt_log):
        return volt_log
    log_dir = os.path.abspath(os.path.join(PV_MOUNTPT, ssname, VOLTDBROOT, 'log'))
    if os.path.exists(log_dir):
        return volt_log
    return None

####
# Usual entry point

if __name__ == "__main__":
    try:
        main()
    except:
        logging.error("Last chance handler: %s", format_exc())
        logging.error("==TERMINATED==")
        sys.exit(-1)
