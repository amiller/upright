# Download #
> [Binary package](http://upright.googlecode.com/files/zookeeper_upright.tar.gz)

> [Get source code from svn](http://code.google.com/p/upright/source/checkout)

# Configuration #
  1. UpRight must be configured properly. See [UpRight Configuration](UpRightConfigurationExecution.md).
  1. Zookeeper also needs to be configured properly. See [Zookeeper Administrator's Guide](http://hadoop.apache.org/zookeeper/docs/current/zookeeperAdmin.html#sc_configuration). We've provided a sample configuration. Notice: Cluster options are not used in Zookeeper-UpRight.


# Run Zookeeper-UpRight #
> You can run it manually or use our script. If you put Zookeeper-UpRight files in different directories on different machines, then you need to do this manually. Otherwise, you can use our script.
## Run manually ##
  * Start UpRight Core as described in [UpRight Configuration](UpRightConfigurationExecution.md).
  * Start Zookeeper server (id is the integer identifier of the server. For the first server, it should be 0. For the second one, it's 1, etc. current.properties is the UpRight configuration file and zoo\_sample.cfg is the Zookeeper configuration file. You need to run this command on all the server machines):
```
java -Djava.library.path=. -cp conf:zookeeper-dev.jar:log4j-1.2.15.jar:bft.jar:FlexiCoreProvider-1.6p3.signed.jar:CoDec-build17-jdk13.jar:netty-3.1.4.GA.jar
org.apache.zookeeper.server.quorum.QuorumPeerMain id current.properties conf/zoo_sample.cfg
```

  * Start client: this depends on your application.
## Run the script ##
> We've provided a script start\_replicated.sh to start all the server nodes, including UpRight Core nodes and Zookeepr server nodes. Still you need to generate membership file current.properties first, and then you need to modify configuration.sh for basic configurations, mainly including some directory information, and then you can execute start\_replicated.sh. Finally, you can go to your first client machine to run generate\_data.sh for a test. If it succeeds, then it is OK. Run stop\_servers.sh to kill all server nodes.