<table align='center'><tr><td>
<h1>Welcome to UpRight</h1>
</td></tr></table>

UpRight is an infrastructure and library for building fault tolerant distributed systems. The goal is to provide a simple library that can ensure that systems remain up (available) and right (correct) despite faults.

UpRight tries to make fault tolerance easy. Typically, connecting an application to the UpRight library requires only a few small changes, after which UpRight can handle the details of replication, fail-over, consistency, etc.

The UpRight replication engine provides the following features:
  * **High Availability**: the system remains available even if as many as u nodes fail (u is a configuration parameter)
  * **High Reliability**: The system remains correct even if as many as r nodes fail, even if these failures are Byzantine (e.g., a hardware error or software bug corrupts a node's state or the messages a node sends to/receives from clients)
  * **High Performance**: the performance of the UpRight version of the system is close to the unreplicated version.

As shown in the following figure, UpRight consists of 1) the UpRight Core, which is a replication engine and 2) the client and server libraries, which connect the application with the UpRight service.

<br />
<table align='center'><tr><td>
<img src='http://www.cs.utexas.edu/~yangwang/UpRight.jpg' />
</td></tr></table>
<br />

During execution, the application client sends its requests through the client library and these requests are ordered by the UpRight Core. The application servers handle these ordered requests and send replies back to the clients. The UpRight replication engine can guarantee that even if a given number of nodes are down, faulty, or even malicious, the whole system can still work correctly.

We have provided demos of UpRight version of [the Hadoop Distributed File System (HDFS)](http://hadoop.apache.org/core/docs/current/hdfs_design.html) and [Zookeeper coordination and lock service](http://hadoop.apache.org/zookeeper). In UpRight-HDFS, the namenode is replicated so that there is no single failure point in the whole system. If UpRight-Zookeeper, we replaced the original Paxos-like consensus protocol, so that it can tolerate Byzantine failures not just crash failures.


# UpRight #
  * [Installation Guide](UpRightInstall.md)
  * [Write a distributed program with UpRight](ProgrammingWithUpRight.md)
  * [Example](UpRightExample.md)
  * [Configuration and Execution](UpRightConfigurationExecution.md)
  * [The paper "UpRight Cluster Services"](http://www.sigops.org/sosp/sosp09/papers/clement-sosp09.pdf)
  * [Advanced Topics](UpRightAdvanced.md)
  * [FAQ](UpRightFAQ.md)
  * [TBD and Known issues](UpRightIssue.md)

# UpRight-HDFS #
  * [Overview](HDFSUpRightOverview.md)
  * [Getting Started](HDFSUpRightGettingStarted.md)
  * [Performance](HDFSUpRightPerformance.md)

# UpRight-Zookeeper #
  * [Getting Started](ZookeeperUpRightGettingStarted.md)
  * [Performance](ZookeeperUpRightPerformance.md)

Notice: Current UpRight-HDFS and UpRight-Zookeeper are demo versions, NOT ready for real deployment.