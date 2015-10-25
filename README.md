Interactive network attacks on UpRight
=====================================

Andrew Miller, Oct 2015

Run with `./run-bug.sh` after building with `ant dist`.

This runs one of the UpRight provided examples, while routing through a python 'adversarial scheduler'. It uses tmux to display several windows in the following pattern:

```
 +-----------------------------------+
 |        |        | exec0 |         |
 |        |        |       |         |
 | order0 | order1 |-------+scheduler|
 |________|________| exec1 |_________|  
 |        |        |       |         |
 |        |        |-------+         |
 | order2 | order3 | exec2 | client  |
 |        |        |       |         |
 +-----------------------------------+ 
```

What this scheduler does:
-------------------------
For the first 25 seconds, all messages are routed correctly. The clients make a total of 6 successful requests. After 25 seconds, the initial leader (node 0) is partitioned.

Expected behavior: partitioning the leader should cause a view change

Observed behavior (see screenshot below): After the leader is partitioned, no new progress is seen.

<img src="http://i.imgur.com/YK2mQW4.png"/>