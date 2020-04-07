#!/usr/bin/env python

import sys
import subprocess


def do_cmd(cmd):
    r = subprocess.check_output(cmd, shell=True).strip()
    R = eval(r)
    return R


# subprocess.check_output was introduced in 2.7, let's be sure.
if sys.hexversion < 0x02070000:
    raise Exception("Python version 2.7 or greater is required.")

RC = 0

r = do_cmd("curl -sg http://localhost:8080/api/2.0/DB-procedure...")
if r[0] != 1:
        print "Database is not ready or not processing transactions, '%s'" % r[1]
        RC = 1

sys.exit(RC)
