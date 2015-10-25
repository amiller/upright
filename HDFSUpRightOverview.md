**_NOTE_** : Current UpRight-HDFS is based on the HDFS revision `r705662` from [HDFS Version Control System](http://hadoop.apache.org/hdfs/version_control.html).

# UpRight-HDFS NameNode #
UpRight-HDFS NameNode enhances HDFS NameNode by eliminating a single point of failure and improving availability by supporting redudant NameNodes with automatic failover and by providing end-to-end Byzatine fault tolerance.
UpRight-HDFS also can be built straightforwardly by using the UpRight framework.
As the figure below illustrates, when u=1 and r=0, UpRight-HDFS replicates the HDFS NameNode to three machines.
Then, even if any of these machines crashes, the system remains available.
Later, when the crashed node recovers, UpRight-HDFS automatically brings its state up to date.
We report on our experience with UpRight-HDFS in a [paper](http://www.sigops.org/sosp/sosp09/papers/clement-sosp09.pdf).
In summary, performance is quite good.

![http://www.cs.utexas.edu/~sangmin/UpRight-HDFS-u1r0_640x480.jpg](http://www.cs.utexas.edu/~sangmin/UpRight-HDFS-u1r0_640x480.jpg)



## NameNodes ##
We seek to minimize changes to the existing HDFS codebase.
As in the original HDFS system, UpRight-HDFS employs a [Primary-Helper approach](ProgrammingWithUpRight.md) to generate checkpoints.  Also, in order to adapt to the UpRight framework, HDFS NameNode is modified to implement [the interface exposed by the UpRight Server Library](InterfaceSpecification.md) and to [eliminate nondeterminism](NonDeterminism.md) in NameNodes.
This requires UpRight-HDFS NameNode replicas to checkpoint their whole state.  Therefore, a NameNode checkpoint includes all soft state in addition to namespace information.
Furthermore, in UpRight-HDFS NameNode, a log is managed by the UpRight Server Library, hence logging
operations provided by the original HDFS are disabled.

## DataNodes and Clients ##
Both DataNodes and HDFS clients are UpRight clients and any requests from them to NameNode should be sent through UpRight Client Library.  Therefore, on each machine on which DataNode or/and HDFS clients run, the UpRight client proxy runs as well.  The proxy includes the UpRight Client Library and exposes a same interface as one that exposed by HDFS NameNode.  DataNode and HDFS clients on the machine send their requests for NameNode to the proxy as if they were sending to the NameNode directly.

# UpRight-HDFS DataNode #
In addition to providing a highly available NameNode, [the UpRight paper](http://www.sigops.org/sosp/sosp09/papers/clement-sosp09.pdf) explores a broader range of Byzantine fault tolerance including modifying the HDFS DataNode to ensure full end-to-end Byzantine fault tolerance.
This optional feature can be enabled or disabled in our prototype.

HDFS can tolerate some DataNode failures but not all Byzantine failures.
For example, if a DataNode suffers a fault that corrupts a disk block but not the corresponding checksum, then a client would detect the error and reject the data, but if a faulty DataNode returns the wrong block and also returns the checksum for that wrong block, a client would accept the wrong result as correct.  UpRight-HDFS DataNode can tolerate more kinds of failures including Byzantine failures.

UpRight-HDFS DataNode do not employ a state machine replication approach mainly for preserving many policies of the original HDFS, such as where/how to put data blocks and how to read blocks.  Instead, UpRight-HDFS DataNode makes a few simple changes to the existing DataNode.  The main changes are
  1. Adding a cryptographic subblock hash on each 64KB subblock of each 64MB(by default) block and a cryptographic block hash across all of a block’s subblock hashes
  1. Storing each block hash at the NameNode.

## Writing Blocks ##
A client sends a block to a set of DataNode, calculates the block hash and includes it in its write complete request or additional block request to the NameNode.
Upon receipt of a block from a client, DataNodes compute and store the subblock hashes and the block hash and include the block hash in its block receiving complete report to the NameNode.
NameNode commits a write only if the client and a sufficient number of DataNodes report the same block hash.
As in the existing code, clients retry on timeout, the NameNode eventually aborts writes
that fail to complete, and the NameNode eventually garbage collects DataNode blocks that
are not included in a committed write.

## Reading blocks ##
To read a block, a client fetches the block hash and list of DataNodes from the NameNode,
fetches the subblock hashes from a DataNode, checks the subblock hashes against the block hash,
fetches subblocks from a DataNode, and finally checks the subblocks against the subblock hashes.
The client retries using a different DataNode if there is an error.