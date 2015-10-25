# This is designed as a routing middle layer for 
import re
import sys
from gevent.server import StreamServer
import gevent.monkey
gevent.monkey.patch_socket()
from gevent import socket
from gevent import Greenlet
from gevent import subprocess

# This script listens on IN_PORTS and forwards messages to OUT_PORTS

# These are the ports that the replicas try to connect to.
IN_PORTS  = range(10000,10008)+range(10010,10018)+ \
             range(10020,10028)+range(10030,10038)
# These are the ports that the replicas are listening on
OUT_PORTS = range(10500,10508)+range(10010,10518)+ \
           range(10520,10528)+range(10030,10538)


# COMMAND = 'GO', None # Deliver all threads
#COMMAND = 'NONE', None
COMMAND = 'SOME', (-1,0,1,2,3,4,5,6,7)  # Isolate some nodes

def allpairs(q):
    for i in q:
        for j in q:
            if i != j: yield i,j

def should_route(src, tgt):
    cmd, args = COMMAND
    if cmd == 'GO': return True
    if cmd == 'NONE': return False
    if cmd == 'SOME':
         if src in args and tgt in args: 
             return True
         else: return False
    # if cmd == 'INTERFERE':
    #     leader, states = args
    #     if leader == src: return False
    #     if leader == tgt: return False
    #     if (src,tgt) in states: return True
    #     return False
    assert False, 'bad command'

ROUTING = []
def router(rd_sock, wr_sock, src, tgt):
    global COMMAND
    # Assumptions:
    #   rd_sock and wr_sock are NON-BLOCKING
    s = ''
    ROUTING.append((src,tgt))
    #if len(ROUTING) == len(OUT_PORTS): print 'All routes established'
    while 1:
        # Wait to read, or 0.1 seconds
        try: 
            socket.wait_read(rd_sock.fileno(), timeout=0.5)
            s += rd_sock.recv(4096)
            if len(s):
                pass
                #print src,tgt, 'read', len(s) 
                #print 'timeout:', 'timeout' in s
                #print 'local:', 'local' in s
        except socket.timeout: pass

        if not should_route(src, tgt):
            gevent.sleep(0.1)
            continue

        # If we have something to write, write some of it
        if not len(s): continue
        socket.wait_write(wr_sock.fileno())
        written = wr_sock.send(s)
        assert written == len(s), 'write was incomplete'
        #print '[%d->%d] delivered len(%d)' % (src,tgt,len(s))
        s = ''

def find_process_connecting_from_port(port):
    # This assumes bftsmart is of the form,
    #   java -cp bin/j.jar bftsmart.demo.counter.CounterServer 3
    # Our goal is to find 3
    log = subprocess.check_output('lsof -i :%d | grep "localhost:%d->"' % (port,port), shell=True)
    pid = int(re.match('^java\s+(\d+)', log).groups()[0])
    cmd = subprocess.check_output('ps ww %d | grep java' % pid, shell=True, universal_newlines=False)
    #idx = int(re.match('.*\s(\d+)$', cmd).groups()[0])
    #print 'Estimated index', idx
    return cmd


def handle_(myport):
    my_idx = (myport % 100) / 10
    def handle(sock, (ip,port)):
        global log
        idx = (myport % 10)-1
        cmd = find_process_connecting_from_port(port)
        #print 'handling', ip, myport, my_idx, idx, port
        #idx = find_bftsmart_process_connecting_from_port(port)
        other = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        newport = myport+500
        #print 'connecting', idx, my_idx, OUT_PORTS[my_idx]
        tries = 0
        while True:
            try:
                #other.connect(('127.0.0.1', OUT_PORTS[my_idx]))
                other.connect(('127.0.0.1', newport))
                break
            except socket.error, e: 
                #print 'connect', idx, my_idx, newport, 'failed'
                gevent.sleep(1)
                tries += 1
                #print 'Retrying', tries
                continue
        sock.setblocking(0)
        other.setblocking(0)
        t1 = Greenlet(run=lambda: router(sock, other, my_idx, idx))
        t2 = Greenlet(run=lambda: router(other, sock, idx, my_idx))
        t1.start()
        t2.start()
        try:
            gevent.joinall((t1,t2))
        finally:
            gevent.killall((t1,t2))
        other.close()
        sock.close()
    return handle

def main():
    global COMMAND

    threads = []
    for i in range(4):
        for j in range(8):
            port = 10000+10*i+j
            t0 = Greenlet(run=StreamServer(('127.0.0.1', port), handle_(port)).serve_forever)
            t0.start()
            threads.append(t0)
            print 'starting', i, j, port

    print '[T+  0]  25 seconds of normal service'
    gevent.sleep(25)
    print '[T+ 25]  Partitioning leader (node 0)'
    COMMAND = "SOME", (-1,1,2,3,4,5,6,7)

    #print '[T+ 25]  Partitioning backup (node 1)'
    #COMMAND = "SOME", (-1,0,2,3,4,5,6,7)
    #gevent.sleep(15)
    #print '[T+ 40]  Partitioning leader (node 0)'
    #COMMAND = "SOME", (-1,1,2,3,4,5,6,7)

    try:
        gevent.joinall(threads)
    finally:
        gevent.killall(threads)
    
if __name__ == '__main__':
    try: __IPYTHON__
    except NameError:
        main()
