First, users need to decide how many and what kinds of faults the system need to tolerate. Let r be the total number of Byzantine faults and u be the total number of faults (including Byzantine faults, so u>=r), then the whole system requires 2u+r+1 ordered nodes and 2u+1 execution nodes. If filtered node is used, it also requires 2u+r+1 filtered nodes. Notice that u and r for different nodes can be different. E.g., you can set u of ordered nodes to 1 and set u of servers to 2.

The configuration requires one membership file and several key files. The membership file defines the public information of all the nodes, including IP, ports, public keys, etc and this file should be distributed to every node. The key files define the private keys of all the nodes and they should only be distributed to the corresponding nodes.

The membership and key files should be in a correct layout. For example, if there are one ordered node, two execution nodes and one client. The file layout should be:
```
test.properties
keys\
  ORDER0.privk.properties   (this file only exists on order node 0)
  EXEC0.privk.properties    (this file only exists on server node 0)
  EXEC1.privk.properties    (this file only exists on server node 1)
  CLIENT0.privk.properties  (this file only exists on client 0)
```
The test.properties is the membership file and all the key files should be in the keys directory.

# Sample Configuration Files #
All these sample configuration files can be found in the sample\_config directory.
  * [1 Order 1 Server 1 Client on localhost](SampleConfig1.md)
  * [1 Order 3 Servers 1 Client on different machines](SampleConfig2.md)
  * [4 Orders 3 Servers 4 Clients on different machines](SampleConfig3.md)
  * [4 Orders 4 Filters 3 Servers 4 Clients on different machines](SampleConfig4.md)

# Generate Configuration Files #
The configuration files seem to be complex? Don't be afraid. We have provided a tool to generate all these files. All you have to do is to decide how many crashes you want to tolerate and which machines you want to use. The tool will generate all the keys, ports and private key files for you. The tool can be found in sample\_config directory.
Steps:
  1. Modify the file "orders". List all the machines you want to use as ordered nodes. Each machine should occupy one line.
  1. Modify the file "servers". List all the machines used as servers.
  1. Modify the file "clients". List all the machines used as clients.
  1. Modify the file "filters". List all the machines used as filtered nodes. Leave it as blank if no filtered nodes are used.
  1. Run bftConfigGenerator.sh to generate the membership and key files. This script takes 8 arguments: 1 u of order nodes; 2 r of order nodes; 3 u of server nodes; 4 r of server nodes; 5 client number; 6 whether use filtered node or not; 7 u of filter nodes; 8 r of filter nodes. 6-8 are optional. If there are no 6-8, no filter nodes are used. For example, to generate a (1 order 3 server 1 client) configuration, the command should be "bftConfigGenerator 0 0 1 1 1 >config.properties". And private key files are put into keys directory automatically.

### Notices ###
  1. The number of nodes in the orders/servers/clients/filters/ files should match with the argument of bftConfigGenerator command. For example, bftConfigGenerator 0 0 1 0 1 requires two server nodes. Then there should be two lines in the servers file. Otherwise, bftConfigGenerator will report an error.
  1. bftConfigGenerator does not support multiple same types of nodes on the same machine, except clients. For example, if you want to put two ordered nodes on the same machine, there will be conflicts in ports. If you really want to do so, you need to modify the membership file manually. bftConfigGenerator DOES support different types of nodes on the same machine, e.g, one ordered node and one server on the same machine. However, this will reduce the availability of the system. Putting multiple clients on a machine is OK.
  1. bftConfigGenerator use ports starting from 6000 for ordered nodes, ports starting from 7000 for servers, ports starting from 8000 for filtered nodes and ports starting from 9000 for clients. If this has conflicts with your other applications, you need to modify bftConfigGenerator manually.

# Run UpRight #
  * Copy membership file to every machine and copy private key files to each corresponding machine.
  * To start ordered nodes:
> > java -Djava.library.path=. -cp conf:bft.jar:FlexiCoreProvider-1.6p3.signed.jar:CoDec-build17-jdk13.jar:netty-3.1.4.GA.jar BFT.order.OrderBaseNode 0 config.properties <br />
> > 0 is the id of this ordered node. See the membership file to see the id of each node. config.properties is the membership file.
  * To stop ordered nodes: just kill the java process.
  * If you have filter nodes, the start and stop process is similar to ordered nodes.
  * To start/stop clients and servers: this depends on the application.