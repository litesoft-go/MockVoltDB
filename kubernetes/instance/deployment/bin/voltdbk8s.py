#!/usr/bin/env python

# Mock (logging) VOLTDB K8S node startup adapter
#
# args are voltdb ... (ie. voltdb start command)
# Invoke from the container ENTRYPOINT, or k8s command
#
#   voltdbk8s.py voltdb start <parms>

import sys, os
import shlex
from time import strftime, gmtime, localtime, timezone
import logging
from traceback import format_exc


# mount point for persistent volume voltdbroot
PV_VOLTDBROOT = os.getenv('VOLTDB_K8S_ADAPTER_PVVOLTDBROOT', "/voltdbroot")


def setup_logging():
    log_format = '%(asctime)s %(levelname)-8s %(filename)14s:%(lineno)-6d %(message)s'
    loglevel = logging.DEBUG
    logger = logging.getLogger()
    # Not sure the next 2 appl;y any longer!
    logger.setLevel(logging.NOTSET)
    logger.propogate = True
    console = logging.StreamHandler()
    console.setLevel(loglevel)
    formatter = logging.Formatter(log_format)
    console.setFormatter(formatter)
    logging.getLogger('').handlers = []
    logging.getLogger('').addHandler(console)

    # print banner
    logging.info("VoltDB K8S CONTROLLER")
    logging.info("GMT is: " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) +
                 " LOCALTIME is: " + strftime("%Y-%m-%d %H:%M:%S", localtime()) +
                 " TIMEZONE OFFSET is: " + str(timezone))
    logging.info("command line: %s" % ' '.join(sys.argv))
    #     for k, v in os.environ.items():
    #         logging.debug("environment: " + k + "=" + v)
    logging.debug(os.getcwd())


def main():
    # setup logger to the console
    setup_logging()

    # See if /voltdbroot (persistent storage mount) is exists
    #     if not os.path.exists(PV_VOLTDBROOT):
    #         logging.error("Persistent volume '%s' is not mounted!!!!" % PV_VOLTDBROOT)
    #         sys.exit(1)

    # check that our args look like a voltdb start command line and only that
    if not sys.argv[2] == 'start':
        logging.error("WARNING: expected voltdb start command but found '%s'" % sys.argv[2])
        sys.exit(1)

    args = sys.argv[:]  # copy

    # build the voltdb start command line
    args = shlex.split(' '.join(args))

    logging.info("VoltDB cmd is '%s'" % ' '.join(args[1:]))
    logging.info("Starting VoltDB...")

    # flush so we see our output in k8s logs
    sys.stdout.flush()
    sys.stderr.flush()

    # fork voltdb
    d = os.path.dirname(args[0])
    os.execv(os.path.join(d, args[1]), args[1:])
    sys.exit(0)

if __name__ == "__main__":
    try:
        main()
    except:
        logging.error(format_exc())
        raise
