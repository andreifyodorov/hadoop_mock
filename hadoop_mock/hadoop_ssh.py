#!/usr/bin/env python

import os
import sys
import subprocess
from os.path import join as pathjoin, basename
from pipes import quote


def main(host, opts, sudo, args):
    ssh_cmd = ["ssh"] + opts + [host]
    if '-files' in args:
        output = subprocess.check_output(ssh_cmd + ["mktemp", "-d", "/tmp/bs-hadoop.XXXXXX"])
        tmp_path = output.rstrip()
        subprocess.check_call(ssh_cmd + ["chmod", "755", tmp_path])

        arg_index = args.index('-files') + 1
        files = args[arg_index].split(",")
        subprocess.check_call(
            ["scp", '-q'] + opts + files + ["rnd07.local:%s" % tmp_path])
        args[arg_index] = ",".join(pathjoin(tmp_path, basename(file)) for file in files)

    try:
        subprocess.check_call(
            ssh_cmd + sudo + ["hadoop"] + [quote(arg) for arg in args])

    except subprocess.CalledProcessError as e:
        sys.exit(e.returncode)

    finally:
        if '-files' in args:
            output = subprocess.check_output(ssh_cmd + ["rm", "-rf", tmp_path])


def cli():
    host = os.environ.get('HADOOP_SSH_HOST')
    if not host:
        sys.stderr.write("%s: Define HADOOP_SSH_HOST\n" % sys.argv[0])
        sys.exit(2)

    opts = os.environ.get('HADOOP_SSH_OPTS') or []
    if opts:
        opts = ['-o', opts]

    sudo = os.environ.get('HADOOP_SSH_SUDO') or []
    if sudo:
        sudo = ['sudo', '-u', sudo]

    main(host, opts, sudo, sys.argv[1:])
