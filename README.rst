hadoop_mock
======

Utility to mock hadoop command-line interface at your home.

Overview
--------

::

	$ export HADOOP_SSH_HOST=rnd07.local
	$ export HADOOP_SSH_OPTS=User=afyodorov
	$ export HADOOP_SSH_SUDO=hdfs

	$ hadoop_ssh fs -ls /flume/logs/bs/2014/09/03

	Found 4 items
	drwxr-xr-x   - hdfs supergroup          0 2014-10-01 16:28 /flume/logs/bs/2014/09/03/20
	drwxr-xr-x   - hdfs supergroup          0 2014-10-01 16:29 /flume/logs/bs/2014/09/03/21
	drwxr-xr-x   - hdfs supergroup          0 2014-10-01 16:31 /flume/logs/bs/2014/09/03/22
	drwxr-xr-x   - hdfs supergroup          0 2014-10-01 16:33 /flume/logs/bs/2014/09/03/23

	$ export HADOOP_MOCK_HDFS_PATH=~/.hdfs_mock
	$ export HADOOP_MOCK_CLI=hadoop_ssh

	$ hadoop_mock mock /flume/logs/bs/2014/09/03
	$ hadoop_mock -fs ls /flume/logs/bs/2014/09/03

	Found 4 items
	drwxr-xr-x   - user group        204 2015-04-20 02:40 /flume/logs/bs/2014/09/03/20
	drwxr-xr-x   - user group        204 2015-04-20 02:40 /flume/logs/bs/2014/09/03/21
	drwxr-xr-x   - user group        204 2015-04-20 02:40 /flume/logs/bs/2014/09/03/22
	drwxr-xr-x   - user group        204 2015-04-20 02:40 /flume/logs/bs/2014/09/03/23

	$ seq 10 | hadoop_mock fs -put - /test.txt
	$ hadoop_mock jar hadoop-streaming -reducer 'wc -l' -input /test.txt -output /output
	$ cat ~/.hdfs_mock/output/*

	      10