The source code of this example can be found in Applications/hashtable. This example application provides a remote hashtable, in which the clients can read or write key-value pairs on the replicated server. To illustrate how to handle nondeterminism in server replicas, we extended the functions of the hashtable a bit: when the server receives a write request, it also records the modification time in the hashtable. Furthermore, it also generates a random number for this key/value pair. The client can read the random number and timestamp through the read request.

# Define Requests and Replies #
First, the programmer must define the request and reply messages between the clients and server. In this example, they are defined in HTRequest.java and HTReply.java. These two files are simple, and this step is no different than on other distributed systems, so we do not describe it in detail. The UpRight library regards all requests and replies as byte arrays, so we also provided Convert.java to convert objects to and from byte arrays.

# Implement the Server #
The server implementation is mainly in HTServer.java. The server class must implement the UpRight server interfaces:
```
public class HTServer implements AppCPInterface{
```

### Data Structures ###
The key data structure is a hashtable: (DataUnit is a triple of value, random number, and timestamp). Notice that for deterministic checkpoints, we use LinkedHashMap instead of HashMap/Hashtable, since iteration on a standard HashMap or Hashtable is nondeterministic in Java.
```
LinkedHashMap<String, DataUnit> ht;
```
To demonstrate how to keep states, we also add an extra variable, which is incremented by one for each write operation.
```
int writeCount=0;
```
The CPAppInterface provides the callback for each function. We will show how to instantiate it later.
```
CPAppInterface generalCP=null;
```
And one string is used to store the directory of the snapshot file.
```
private String syncDir=null;
```
### Constructor ###
Initialize everything:
```
        public HTServer(String syncDir, int id) throws IOException {
                this.syncDir=syncDir+File.separator;
                this.id=id;
                ht = new LinkedHashMap<String, DataUnit>();
                File syncDir2 = new File(this.syncDir);
                syncDir2.mkdirs();
        }

```

### Execute a request ###
If it is a read request, then get the data from the hashtable. If it is a write request, then modify the hashtable and increase writeCount. Call execDone finally.
  1. This function shows how to use random number and time deterministically: use RequestInfo.getRandom() and RequestInfo.getTime() instead of using java library functions.

```
        @Override
        public synchronized void execAsync(byte[] request, RequestInfo info) {
                HTRequest req = (HTRequest)(Convert.bytesToObject(request));
                HTReply rep = null;
                String key = req.getKey();
                if(req.getType() == HTRequest.READ){
                        if(ht.containsKey(key)){
                                rep = new HTReply(false, ht.get(key));
                        }else{
                                rep = new HTReply(true, null);
                        }
                } else {        // WRITE operation
                        DataUnit data=new DataUnit(req.getValue(), info.getRandom(), info.getTime());
                        ht.put(key, data);
                        writeCount++;
                        rep = new HTReply(false, null);
                }
                generalCP.execDone(Convert.objectToBytes(rep), info);


        }
```

The read only optimization is not used in this example, so leave execReadonly not implemented. For more information on the read only optimization, see [Server Interface](InterfaceSpecification.md).
```
        @Override
        public void execReadonly(byte[] request, int clientId, long requestId){
                throw new RuntimeException("Not implemented");
        }
```


### Write snapshot to Disk ###
This function writes the application state to the disk. The application should be able to fully recover from this snapshot. In this example, both the hashtable and the writeCount are written to disk. It calls syncDone finally. Both the name and the content of the checkpoint file must be deterministic.
```
        @Override
        public void sync() {
                try {
                        File syncFile=new File(this.syncDir+"ht_sync_"+writeCount);
                        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(syncFile));
                        oos.writeObject(ht);
                        oos.writeInt(writeCount);
                        oos.close();
                }
                catch (Exception e) {
                        e.printStackTrace();
                }
                generalCP.syncDone(this.syncDir+"ht_sync_"+writeCount);
        }
```


### Load the Checkpoint ###
This function is performed during recovery. In this example, the application reads the hashtable and the writeCount from the checkpoint file.
```
        @Override
        public synchronized void loadSnapshot(String fileName) {
                try {
                        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileName));
                        ht = (LinkedHashMap<String, DataUnit>) ois.readObject();
                        writeCount = ois.readInt();
                } catch (Exception e) {
                        e.printStackTrace();
                }

        }
```

### Connect Application server to UpRight ###
The main function shows how to connect the application server to UpRight. First, it creates the GeneralCP class object, which implements CPAppInterface. Then it creates the two server instances and finally calls setupApplication to register them to UpRight. This will startup the UpRight state machine.
```
        public static void main(String args[]) throws Exception{
                if(args.length!=4){
                        System.out.println("Usage: java Applications.hashtable <id> <config_file> <log_path> <snapshot_path>");
                }
                GeneralCP generalCP = new GeneralCP(Integer.parseInt(args[0]), args[1], args[2], args[3]);
                HTServer main = new HTServer(args[3],Integer.parseInt(args[0]));
                main.setGenCP(generalCP);
                generalCP.setupApplication(main);

        }
```

# Implement the Client #
The client code HTClient.java is simple. It uses ClientShimBaseNode.execute to send requests and then handle the reply.
```
package Applications.hashtable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import BFT.clientShim.ClientShimBaseNode;
import BFT.network.TCPNetwork;



public class HTClient{
        ClientShimBaseNode clientShim;
        public HTClient(String membership, int id){
                clientShim = new ClientShimBaseNode(membership, id);
                clientShim.setNetwork(new TCPNetwork(clientShim));
                clientShim.start();
        }

        public void write(String key, int value)
        {
                HTRequest req = new HTRequest(HTRequest.WRITE, key, value);
                byte[] replyBytes = clientShim.execute(Convert.objectToBytes(req));
                HTReply rep=(HTReply)(Convert.bytesToObject(replyBytes));
                if(rep.isError()){
                        throw new RuntimeException("Write failed");
                }
        }

        public DataUnit read(String key){
                HTRequest req = new HTRequest(HTRequest.READ, key, 0);
                byte [] replyBytes = clientShim.execute(Convert.objectToBytes(req));
                HTReply rep = (HTReply)(Convert.bytesToObject(replyBytes));
                if(rep.isError()){
                      throw new RuntimeException("Read failed");
                }
                return rep.getData();
        }

        public static void main(String[] args){
                String membership=args[0];
                int id=Integer.parseInt(args[1]);
                HTClient client=new HTClient(membership, id);
                client.write("1",1);
                DataUnit data=client.read("1");
                System.out.println("value="+data.getValue());
                System.out.println("random="+data.getRandom());
                System.out.println("timestamp="+data.getTimestamp());
        }
}
```

# Compile and Run the example #
  * [Compile your code with UpRight](UpRightInstall.md).
  * [Generate your configuration](UpRightConfigurationExecution.md). In the following steps, we assume you use the (1 order, 1 server, 1 client) configuration on localhost.
  * Start the UpRight Core Order nodes:
```
    java -cp conf:upright.jar:FlexiCoreProvider-1.6p3.signed.jar:CoDec-build17-jdk13.jar BFT.order.OrderBaseNode 0 test.properties
```
  * Start the server:
```
    java -cp conf:upright.jar:FlexiCoreProvider-1.6p3.signed.jar:CoDec-build17-jdk13.jar  Applications.hashtable.HTServer 0 test.properties /tmp/httest/0/log /tmp/httest/0/ss
```

  * Start the client:
```
    java -cp conf:upright.jar:FlexiCoreProvider-1.6p3.signed.jar:CoDec-build17-jdk13.jar Applications.hashtable.HTClient test.properties 0
```
> > You should be able to see it outputs a value 1, a random number and a timestamp.


> In the unreplicated configuration, all nodes have index 0. To run a replicated version, you just need to start all nodes on every machine, with the same command and the correct index number.