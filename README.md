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
