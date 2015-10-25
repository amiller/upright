UpRight provides libraries for both application clients and servers. By connecting to these libraries, the application is incorporated into the UpRight infrastructure. The programmer just needs to provide the functions of a single server application, and then the server is automatically replicated by UpRight.

# Model #

## State machine replication ##
UpRight replicates the application servers for fault tolerance. It requires u+max(u,r)+1 replicas to tolerate up to u faults of which r may be Byzantine. Typically, u is greater than or equal to r, so the number would be 2u+1.
Notice: (u=1,r=0) and (u=1,r=1) configurations both require 3 servers, but (u=1,r=0) will have better performance than (u=1,r=1), since it does not need to handle Byzantine failure.

## Requirements for state machine replication ##
  * **Ordered requests**: requests from all clients must be executed in the same order on all server replicas. Ordered requests also provides strong consistency and linearizability for the system. The order nodes (2u+r+1 nodes) in the UpRight Core guarantee that the requests arrive at all the replicas in the same order. All the server replicas need to do is to execute these requests and send responses in this order. When b>0, UpRight compares the responses from different replicas. If one response is different from the others, it is considered to be incorrect. To be accurate, UpRight requires at least b+1 identical responses for each request.
  * **Deterministic execution**: all replicas must give the same response for the same request. This means that servers must execute each request in a deterministic way. There should be no randomness in the execution. See [common sources of nondeterminism and tips about how to solve them](NonDeterminism.md).
  * **Deterministic checkpoints**: UpRight also requires all replicas to provide the same checkpoints at the same logical time. Therefore, checkpoints must also be deterministic. Since producing a checkpoint usually takes a long time, usually it is necessary to execute requests and produce a checkpoint concurrently. It can be quite challenging for a server to generate a deterministic checkpoint while still processing other requests. The simplest approach is a **blocking checkpoint**, which blocks execution while performing checkpoints, but this approach may introduce unacceptable latencies. UpRight suggests two kinds of asynchronous checkpoint techniques, if blocking checkpoints are not affordable: 1) **Primary-Helper**: a primary instance executes the request and passes the requests to the helper instance. While performing the checkpoint, the helper instance writes all states to the checkpoint file and the primary instance can still keep executing requests . 2) **Application Copy-on-Write (COW)**: if you're willing to spend more time, application COW is an efficient way to do asynchronous checkpoint. How to do it actually depends on the application. The UpRight library provides support for the Primary-Helper approach.
  * **Client-oblivious recovery**: if a replica crashes and then recovers from the checkpoints and logs, it should still be able to provide the same responses and checkpoints as the other replicas. A client should not see any difference between a recovered server and a normal server. This often requires applications to keep additional information as persistent state. For example, for a stateful service, the server should also keep the session information of each client in its log and checkpoint, so that when it recovers, it can still serve these clients correctly.

![http://www.cs.utexas.edu/~yangwang/UpRightModel.jpg](http://www.cs.utexas.edu/~yangwang/UpRightModel.jpg)

## Architecture and Procedure ##

The architecture is shown in the figure. UpRight works as follows:
  1. The client issues a request to the client library.
  1. The UpRight core agrees on the order of requests from all clients.
  1. The UpRight core sends requests to server library in order.
  1. The server library sends requests to the application server, which will execute the request and send the reply back to the server library.
  1. The server library sends the reply to client library.
  1. The client library sends the request to UpRight Core and waits for the reply. The client library returns the result to the client once it has a sufficient number (b+1) of matching replies to guarantee correctness.
  1. Periodically, the server library asks the application server to perform checkpoint. If a server crashes, falls behind, or has its state corrupted, the UpRight library will fetch a correct checkpoint and log of subsequent requests from other replicas to restore the faulty or slow server.

# Creating an UpRight Service #
  1. Server: To be incorporated into the UpRight system, the application server needs to implement a interface **AppCPInterface**. This interface defines the functionality of the application, including request execution, checkpoint, and recovery.
  1. Client: The client library provides an interface to send requests. The application client should use this interface instead of sending requests via sockets.
For details of these interfaces, see [interface specification](InterfaceSpecification.md). <br />
We have provided an example of how to build an UpRight Service. It is a remote hashtable, in which clients can set or get key/value pairs on the server. The example is rather simple, but has all the features described above. [Go to the example](UpRightExample.md).

