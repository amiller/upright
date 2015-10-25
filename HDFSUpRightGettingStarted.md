# Download #
You need to donwload both the [UpRight library](http://upright.googlecode.com/files/upright.tar.gz) and the [UpRight-HDFS](http://upright.googlecode.com/files/hdfs-upright-0.1.tar.gz).

# Configuration #

  1. UpRight must be configured properly. See [UpRight Configuration](UpRightConfigurationExecution.md). Notice that only namenode is replicated by UpRight, so NameNodes are "UpRight servers" and DataNodes are "UpRight clients". Therefore, when configuring UpRight, you should list all DataNodes as clients.
  1. HDFS also needs to be configured properly. See [Hadoop Configuration](http://hadoop.apache.org/core/docs/current/cluster_setup.html#Configuration).
  1. Modify hadoop-site.xml to add the following two properties.
    * _dfs.bft_ : whether to use UpRight replication for namenode or not.
    * _dfs.bft.clientGlue.rpcport_ : a port number used by a UpRight client proxy to run its rpc server
    * _dfs.bft.datanode_ : whether to use UpRight datanode or not.

```
  <property>
    <name>dfs.bft</name>
    <value>true</value>
  </property>

 <property>
    <name>dfs.bft.clientGlue.rpcport</name>
    <value>8787</value>
  </property>

  <property>
    <name>dfs.bft.datanode</name>
    <value>true</value>
  </property>

```

# Run UpRight-HDFS #
Do not use `start-all.sh`, `stop-all.sh`, `start-dfs.sh` and `stop-dfs.sh` to start/stop UpRight-HDFS. Instead, please follow the instructions below.
## Start UpRight Core ##
First, start UpRight Core as described in [UpRight Configuration](UpRightConfigurationExecution.md).
## Start namenodes ##
On each namenode machine, we need to run a primary namenode and a helper namenode. Before executing them, we need to format HDFS:
> `bin/hadoop [--config <PathToHDFSConfigDir>] namenode -format`
Also format HDFS for the helper:
> `bin/hadoop [--config <PathToHDFSConfigDir>] namenode -helper -format`
Then start a helper namenode first:
> `bin/hadoop [--config <PathToHDFSConfigDir>] namenode -helper &`
And finally start a primary namenode:
> `bin/hadoop [--config <PathToHDFSConfigDir>] namenode -shimid <UpRightServerID> -uprightconfig <PathToUpRightConfigFile> &`

Typically you will also want to redirect stdout and stderr of primary/helper namenode to some log files. The following is an example of sequence of commands to run the first NameNode replica:
```
 Formatting HDFS
 $ bin/hadoop namenode -format
 $ bin/hadoop namenode -helper -format

 Starting a helper NameNode
 $ bin/hadoop namenode -helper 2>&1 logs/helper_0.log &

 Starting a primary NameNode
 $ bin/hadoop namenode -shimid 0 -uprightconfig ./uprightConfig 2>&1 logs/primary_0.log  &
```

## Start UpRight client proxies ##
On each machine on which a HDFS DataNode or HDFS clients run,
> `bin/hadoop [--config <PathToHDFSConfigDir>] uprightclientproxy <UpRightClientID> <PathToUpRightConfigFile>`
Following is an example to run an UpRight client proxy:
```
$ bin/hadoop uprightclientproxy 0 ./uprightConfig 2>&1 logs/clientproxy_0.log  &
```

## Start datanodes and clients ##
At this point, you can run DataNodes and any applications that run on HDFS in the same way as the original HDFS.