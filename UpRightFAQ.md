  * What kind of applications need UpRight?
> Client/Server model applications requiring high availability are usually a good target for using UpRight. UpRight can provide Byzantine Fault Tolerance, strong consistency, linearizability and failure recovery with acceptable performance.

  * What kind of applications do not need UpRight?
> If your application does not need high availability, then of course it does not need UpRight. UpRight is based on the Client/Server model. If your application uses other architectures, such as Peer To Peer (P2P), it is unclear whether you can use UpRight for that or not.

  * What are the advantages of UpRight over other crash failure mechanisms?
> Most existing crash failure mechanisms assume the fail/stop model, in which if the server fails, it stops to respond. In reality, we see many cases in which a failed server gives incorrect response instead of just halting. The traditional crash failure mechanism cannot handle this kind of Byzantine Faults. UpRight can provide Byzantine Fault Tolerance for the application automatically.

  * What is the cost of UpRight?
> To tolerate u failures, among which r failures are Byzantine, the whole system requires u+max(u,r)+1 server replicas, 2u+r+1 order nodes and u+r+max(u,r)+1 filter nodes (optional for performance improvement).

  * How should I install UpRight?
> See [Installation Guide](UpRightInstall.md)

  * How should I write programs with UpRight?
> See [Programming with UpRight](ProgrammingWithUpRight.md) and [Programming Example](UpRightExample.md)

  * How to run the UpRight program?
> See [UpRight Configuration and Execution](UpRightConfigurationExecution.md)

  * What is the major difficulty in using UpRight?
> As far as we know, eliminating nondeterminisms in the application is the most challenging part when programming with UpRight.

  * What is NonDetermism and how to eliminate it?
> See [common sources of nondetermism and how to elimate it](NonDeterminism.md)

  * My UpRight program does not run correctly. What is the problem?
> Nondeterminisms, especially in checkpoint and recovery, are the most common causes of problems in our experience. You can check the logs for that. If you have further problems, you can email your logs and exceptions to us.