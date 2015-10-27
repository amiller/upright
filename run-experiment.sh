#!/bin/bash

# Run one of the UpRight provided examples
# Uses tmux to display several windows
# 
# +-----------------------------------+
# |        |        | exec0 |         |
# |        |        |       |         |
# | order0 | order1 |-------+ filter  |
# |________|________| exec1 |_________|  
# |        |        |       |         |
# |        |        |-------+         |
# | order2 | order3 | exec2 | client  |
# |        |        |       |         |
# +-----------------------------------+ 
#

export JAVA_OPTS="-cp conf:dist/lib/bft.jar:lib/FlexiCoreProvider-1.6p3.signed.jar:lib/CoDec-build17-jdk13.jar:lib/netty-3.1.4.GA.jar"

echo java ${JAVA_OPTS} BFT.order.OrderBaseNode 0 config.properties
tmux new-session    "sleep 3; bash -c 'java ${JAVA_OPTS} BFT.order.OrderBaseNode 0 config.properties | tee order0.log'; bash" \;  \
    splitw -h -p 75 'sleep 3; java ${JAVA_OPTS} BFT.order.OrderBaseNode 1 config.properties | tee order1.log; bash' \;  \
    splitw -h -p 66 'sleep 1; java ${JAVA_OPTS} Applications.hashtable.HTServer 0 config.properties ./httest/0/log ./httest/0/ss | tee exec0.log' \; \
    splitw -h -p 50 'sleep 6; java ${JAVA_OPTS} BFT.filter.FilterBaseNode 0 config.properties | tee filter0.log' \; \
    selectp -t 2 \; \
    splitw -v -p 66 'sleep 1; java ${JAVA_OPTS} Applications.hashtable.HTServer 1 config.properties ./httest/1/log ./httest/1/ss | tee exec1.log' \; \
    splitw -v -p 50 'sleep 1; java ${JAVA_OPTS} Applications.hashtable.HTServer 2 config.properties ./httest/2/log ./httest/2/ss | tee exec2.log' \; \
    selectp -t 5 \; \
    splitw -v -p 50 'sleep 7; for i in `seq 20`; do java ${JAVA_OPTS} Applications.hashtable.HTClient config.properties 0 | tee -a client0.log; sleep 1; done; bash' \; \
    selectp -t 0 \; \
    splitw -v -p 50 'sleep 3; java ${JAVA_OPTS} BFT.order.OrderBaseNode 2 config.properties | tee order2.log; bash' \;  \
    selectp -t 2\; \
    splitw -v -p 50 'sleep 3; java ${JAVA_OPTS} BFT.order.OrderBaseNode 3 config.properties | tee order3.log; bash'
