#!/bin/bash

# Run one of the UpRight provided examples
# Uses tmux to display several windows
# 
# +-----------------+
# |        |        |
# | order0 | filter |
# |________|________|
# |        |        |
# | exec   | client |
# |        |        |
# +-----------------+
#

export JAVA_OPTS="-cp conf:dist/lib/bft.jar:lib/FlexiCoreProvider-1.6p3.signed.jar:lib/CoDec-build17-jdk13.jar:lib/netty-3.1.4.GA.jar"

echo java ${JAVA_OPTS} BFT.order.OrderBaseNode 0 config.properties
tmux new-session    "sleep 1; bash -c 'java ${JAVA_OPTS} BFT.order.OrderBaseNode 0 config.properties | tee order0.log'; bash" \;  \
    splitw -h -p 50 'sleep 0; java ${JAVA_OPTS} BFT.filter.FilterBaseNode 0 config.properties | tee filter0.log' \; \
    splitw -v -p 50 'sleep 7; for i in `seq 20`; do java ${JAVA_OPTS} Applications.hashtable.HTClient config.properties 0 | tee -a client0.log; sleep 1; done; bash' \; \
    selectp -t 0 \; \
    splitw -v -p 50 'sleep 3; java ${JAVA_OPTS} Applications.hashtable.HTServer 0 config.properties ./httest/0/log ./httest/0/ss | tee exec0.log'
