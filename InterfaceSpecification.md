# Server Interface #
The application should implement **BFT.generalcp.AppCPInterface**. It includes four functions:
  * execAsync: execute a request. After finished, the application should call CPAppInterface.execDone to send the result back.
  * execReadonly: execute a read only request. UpRight provides optimization for read only requests and they are much faster than normal requests. After finished, the application should call CPAppInterface.execReadonlyDone to send the result back. Typically, executing a read only request is no different than executing a normal one, except that the programmer has to make sure it is really read only. See following Notices for more detail.
  * sync: write all the application states into a snapshot file. Call CPAppInterface.syncDone when finished.
  * loadSnapshot: load the snapshot file. This is a blocking call.

The UpRight library provides the **BFT.generalcp.CPAppInterface** for the application to communicate with the UpRight library:
  * setupApplication: register the application server to the UpRight library. **This function starts the UpRight server library**.
  * execDone: callback for execAsync
  * execReadonlyDone: callback for execReadonly
  * syncDone: callback for sync
  * getIP, getPort: get the client address information of a specific client
  * sendEvent: send a message to the client. Application should call this function only if the message is NOT a reply of a request. Otherwise, the application should call execDone or execReadonlyDone.



## Notices ##
  * A **readonly** request is NOT simply a read request. It should not modify any states in the application server, which will to be written to the snapshot file. For example, if you keep a counter for read requests and the counter is written to the snapshot file, then the read is not readonly. If you just keep the counter in memory and the counter does not affect any responses, then it is OK.
  * The application server should never modify, delete, rename or move a snapshot file after returning it to UpRight by calling syncDone. The UpRight library will perform the garbage collection.
  * The application must provide deterministic snapshot files. There should be no randomness in these files. In reality, time, multi thread, etc, can cause non-determinisms and they should be carefully avoided.

# Application Client #
UpRight takes care of the network communication, so the client should not touch any socket code any more. Instead, UpRight provides a interface **ClientShimInterface** for clients to send requests:
  * execute: the request will be passed to the application server through the execAsync interface. Currently, execute is a blocking call and it will wait for the reply from the server.
  * executeReadonly: the request will be passed to the application server through the execReadonly interface.