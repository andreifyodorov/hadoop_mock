#!/usr/bin/env python

import sys
import os
import subprocess
import argparse
import glob
import datetime
import shutil
import pipes


HDFS_MOCK_PATH = os.environ.get('HADOOP_MOCK_HDFS_PATH') or os.path.expanduser('~/.hdfs_mock')


def run_calls(calls):
    active_childs = []
    for call in calls:
        pid = os.fork()
        if pid:
            active_childs.append(pid)
        else:
            subprocess.check_call(**call)
            sys.exit(0)
        if len(active_childs) == 10:
            pid, status = os.wait()
            active_childs.remove(pid)
    while active_childs:
        pid, status = os.wait()
        active_childs.remove(pid)


def hdfs_path(path):
    return path[len(HDFS_MOCK_PATH):]


def local_path(path):
    return os.path.join(HDFS_MOCK_PATH, path[1:])


def mock(args):
    parser = argparse.ArgumentParser(prog="hadoop_mock mock", add_help=False)
    parser.add_argument('--head', default=10000)
    parser.add_argument('path', nargs="+")
    args = parser.parse_args(args)

    hadoop_cli = os.environ.get('HADOOP_MOCK_CLI')
    if not hadoop_cli:
        parser.error("please define HADOOP_MOCK_CLI env variable")

    files = subprocess.check_output([hadoop_cli, 'fs', '-ls', '-R'] + args.path)
    calls = []
    for row in files.split("\n"):
        if not row:
            continue
        mode, rf, user, group, size, date, time, fname = row.split()
        if mode.startswith('d'):
            dst = None
            dst_dir = local_path(fname)
        else:
            dst = local_path(fname)
            dst_dir = os.path.dirname(dst)
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
        if dst:
            calls.append(dict(
                args='./hadoop_ssh fs -cat %s | head -%d > %s' % (fname, args.head, dst),
                shell=True
            ))
    run_calls(calls)


def print_file_info(path):
    is_dir = os.path.isdir(path)
    stat = os.stat(path)
    dt = datetime.datetime.fromtimestamp(stat.st_mtime)
    print "%srwxr-xr-x   %s user group %10s %s %s %s" % (
        'd' if is_dir else '-',
        '-' if is_dir else '3',
        stat.st_size,
        dt.strftime("%Y-%m-%d"),
        dt.strftime("%H:%M"),
        hdfs_path(path))


class Curse(object):
    def __init__(self, ident):
        self.ident = ident
        self.status = 0

    def __call__(self, message, file=None):
        if file:
            message = "`%s': %s" % (file, message)
        sys.stderr.write("%s: %s\n" %(self.ident, message))
        self.status = 1
        return self.status


def render_wildcards(pathes, curse):
    for path in pathes:
        files = glob.glob(local_path(path))
        if files:
            for file in files:
                yield file
        else:
            curse("No such file or directory", path)


def ls(args):
    parser = argparse.ArgumentParser(prog="hadoop_mock fs -ls", add_help=False)
    parser.add_argument("-d", action="store_true", dest="dirs_only")
    parser.add_argument("-R", action="store_true", dest="recursive")
    parser.add_argument("path", nargs="+")
    args = parser.parse_args(args)

    if args.recursive:
        raise NotImplementedError("-R not implemented")

    curse = Curse('ls')
    for file in render_wildcards(args.path, curse):
        if os.path.isdir(file) and not args.dirs_only:
            dirlist = glob.glob(os.path.join(file, '*'))
            if dirlist:
                print "Found %d items" % len(dirlist)
                for file in dirlist:
                    print_file_info(file)
        else:
            print "Found 1 items\n"
            print_file_info(file)
    return curse.status


def rm(args):
    parser = argparse.ArgumentParser(prog="hadoop_mock fs -rm", add_help=False)
    parser.add_argument("-f", action="store_true", dest="force")
    parser.add_argument("-R", "-r", action="store_true", dest="recursive")
    parser.add_argument("path", nargs="+")
    args = parser.parse_args(args)

    if args.force:
        raise NotImplementedError("-f not implemented")

    curse = Curse('rm')
    for file in render_wildcards(args.path, curse):
        if os.path.isdir(file) and not args.recursive:
            curse('Is a directory', hdfs_path(file))
        else:
            print "Removed: '%s'\n" % file
            shutil.rmtree(file)
    return curse.status


def mkdir(args):
    parser = argparse.ArgumentParser(prog="hadoop_mock fs -mkdir", add_help=False)
    parser.add_argument("path", nargs="+")
    parser.add_argument("-p")
    args = parser.parse_args(args)

    curse = Curse('mkdir')
    for path in args.path:
        local = local_path(path)
        if os.path.exists(local):
            curse('File exists', path)
        else:
            os.makedirs(local)
    return curse.status


def mv(args):
    parser = argparse.ArgumentParser(prog="hadoop_mock fs -mv", add_help=False)
    parser.add_argument("path", nargs="+")
    parser.add_argument("dest")
    args = parser.parse_args(args)

    curse = Curse('mv')

    local_dest = local_path(args.dest)
    if len(args.path) == 1:
        if not os.path.exists(os.path.dirname(local_dest)):
            return curse('No such file or directory', args.dest)
        if os.path.isfile(local_dest):
            return curse('File exists', args.dest)
    else:
        if not os.path.exists(local_dest):
            return curse('No such file or directory', args.dest)
        if not os.path.isdir(local_dest):
            return curse('Is not a directory', args.dest)
    if os.path.isdir(local_dest):
        make_dest = lambda fname: os.path.join(local_dest, os.path.basename(fname))
    else:
        make_dest = lambda fname: local_dest

    for path in args.path:
        local = local_path(path)
        if not os.path.exists(local):
            curse('No such file or directory', hdfs_path(path))
        else:
            os.rename(local, make_dest(path))

    return curse.status


def put(args):
    parser = argparse.ArgumentParser(prog="hadoop_mock fs -put", add_help=False)
    parser.add_argument("localsrc", nargs="+", type=argparse.FileType('r'))
    parser.add_argument("dest")
    args = parser.parse_args(args)

    curse = Curse('put')

    local_dest = local_path(args.dest)
    if os.path.exists(local_dest):
        return curse('File exists', args.dest)
    dst_dir = os.path.dirname(local_dest)
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)

    fds = ["/dev/fd/%d" % src.fileno() for src in args.localsrc]
    subprocess.check_call("cat %s > %s " % (" ".join(fds), local_dest), shell=True)

    return curse.status


def cat(args):
    parser = argparse.ArgumentParser(prog="hadoop_mock fs -cat", add_help=False)
    parser.add_argument("paths", nargs="+")
    args = parser.parse_args(args)

    curse = Curse('cat')
    paths = render_wildcards(args.paths, curse)
    for path in paths:
        if os.path.isdir(path):
            curse('Is a directory', hdfs_path(path))
        else:
            subprocess.check_call(["cat", path])
    return curse.status


fs_cmds = {
    'ls': ls,
    'rm': rm,
    'mkdir': mkdir,
    'mv': mv,
    'put': put,
    'cat': cat
}


def fs(args):
    parser = argparse.ArgumentParser(prog="hadoop_mock fs", add_help=False)
    group = parser.add_mutually_exclusive_group(required=True)
    for key in fs_cmds.iterkeys():
        group.add_argument("-%s" % key, action='store_true')

    args, leftovers = parser.parse_known_args(args)

    for key, func in fs_cmds.iteritems():
        if getattr(args, key):
            return func(leftovers)


def streaming_script(cmd, files_map):
    args = None
    if " " in cmd:
        cmd, args = cmd.split(None, 1)
    cmd = files_map.get(cmd, cmd)
    if args:
        cmd = "%s %s" % (cmd, args)
    return cmd


class ConcatFiles(object):
    def __init__(self, files):
        self.files = iter(files)
        self.current = next(self.files)

    def read(self, size):
        if self.current is None:
            return str()
        result = self.current.read(size)
        while len(result) < size:
            try:
                self.current = next(self.files)
            except StopIteration:
                self.current = None
                break
            result += self.current.read(size - len(result))
        return result

    def __iter__(self):
        while self.current:
            yield self.read(8192)


def filehandles(inputs):
    for file in inputs:
        with open(file) as fh:
            yield fh


def bash_popen(cmd, **kwargs):
    return subprocess.Popen(["/bin/bash", "-o", "errexit", "-o", "pipefail", "-c", cmd], **kwargs)


class MapreduceError(Exception):
    pass


def mapper(cmd, inputs):
    for file in inputs:
        env = dict(os.environ)
        env['map_input_file'] = hdfs_path(file)
        with open(file) as stdin:
            p = bash_popen(cmd, stdin=stdin, stdout=subprocess.PIPE, env=env)
            yield p.stdout
        p.wait()
        if p.returncode > 0:
            raise MapreduceError


def run_mapreduce(mapped, cmd):
    p = bash_popen(cmd, stdin=subprocess.PIPE)
    for chunk in mapped:
        p.stdin.write(chunk)
    p.stdin.close()
    p.wait()
    if p.returncode > 0:
        raise MapreduceError


def streaming(args):
    parser = argparse.ArgumentParser(prog="hadoop_mock jar", add_help=False)
    parser.add_argument("jar")
    parser.add_argument("-files")
    parser.add_argument("-D", action="append")
    parser.add_argument("-mapper")
    parser.add_argument("-partitioner")
    parser.add_argument("-reducer")
    parser.add_argument("-input", action="append", required=True)
    parser.add_argument("-output", required=True)
    args = parser.parse_args(args)

    if 'hadoop-streaming' not in args.jar:
        parser.error("expected jar to be 'hadoop-streaming'")

    files_map = {}
    if args.files:
        files_map.update((os.path.basename(file), file) for file in args.files.split(','))

    inputs = []
    for path in args.input:
        path = local_path(path)
        if os.path.isdir(path):
            inputs.extend(filter(os.path.isfile, glob.glob("%s/*" % path)))
        else:
            inputs.append(path)

    output = local_path(args.output)
    if os.path.exists(output):
        sys.stdout.write('Path %s exists\n' % output)
        return 1

    if args.mapper:
        mapped = ConcatFiles(mapper(streaming_script(args.mapper, files_map), inputs))
    else:
        mapped = ConcatFiles(filehandles(inputs))

    cmds = ["sort"]
    if args.reducer:
        cmds.append(streaming_script(args.reducer, files_map))

    cmd = "%s > %s" % (" | ".join(cmds), os.path.join(output, 'part-00000'))

    os.makedirs(output)
    try:
        run_mapreduce(mapped, cmd)
    except MapreduceError:
        shutil.rmtree(output)
        return 1


def cli():
    parser = argparse.ArgumentParser(prog="hadoop_mock", add_help=False)
    parser.add_argument('mode', choices=['mock', 'fs', 'jar'])
    args, leftovers = parser.parse_known_args()
    if args.mode == "mock":
        sys.exit(mock(leftovers))

    elif args.mode == "fs":
        sys.exit(fs(leftovers))

    elif args.mode == "jar":
        sys.exit(streaming(leftovers))
