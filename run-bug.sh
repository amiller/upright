#!/bin/bash

# Run one of the UpRight provided examples
# Also runs the python 'adversarial scheduler'
# Uses tmux to display several windows
# 
# +-----------------------------------+
# |        |        | exec0 |         |
# |        |        |       |         |
# | order0 | order1 |-------+scheduler|
# |________|________| exec1 |_________|  
# |        |        |       |         |
# |        |        |-------+         |
# | order2 | order3 | exec2 | client  |
# |        |        |       |         |
# +-----------------------------------+ 
#

export JAVA_OPTS="-Djava.library.path=. -cp conf:bft.jar:FlexiCoreProvider-1.6p3.signed.jar:CoDec-build17-jdk13.jar:netty-3.1.4.GA.jar"

echo java ${JAVA_OPTS} BFT.order.OrderBaseNode 0 config.properties
tmux new-session    'sleep 2; java ${JAVA_OPTS} BFT.order.OrderBaseNode 0 config.properties; bash' \;  \
    splitw -h -p 75 'sleep 2; java ${JAVA_OPTS} BFT.order.OrderBaseNode 1 config.properties; bash' \;  \
    splitw -h -p 66 'sleep 1; java ${JAVA_OPTS} Applications.hashtable.HTServer 0 config.properties ./httest/0/log ./httest/0/ss' \; \
    splitw -h -p 50 'python amiller-bug.py' \; \
    selectp -t 2 \; \
    splitw -v -p 66 'sleep 1; java ${JAVA_OPTS} Applications.hashtable.HTServer 1 config.properties ./httest/1/log ./httest/1/ss' \; \
    splitw -v -p 50 'sleep 1; java ${JAVA_OPTS} Applications.hashtable.HTServer 2 config.properties ./httest/2/log ./httest/2/ss' \; \
    \
    selectp -t 5 \; \
    splitw -v -p 50 'sleep 5; for i in `seq 10`; do java ${JAVA_OPTS} Applications.hashtable.HTClient config.properties 0; done; bash' \; \
    selectp -t 0 \; \
    splitw -v -p 50 'sleep 2; java ${JAVA_OPTS} BFT.order.OrderBaseNode 2 config.properties; bash' \;  \
    selectp -t 2\; \
    splitw -v -p 50 'sleep 2; java ${JAVA_OPTS} BFT.order.OrderBaseNode 3 config.properties; bash'
