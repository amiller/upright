// $Id$

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import BFT.generalcp.*;
import org.apache.zookeeper.server.util.ConnectionObserver;
import org.apache.zookeeper.server.util.ObservableComponent;
import org.apache.zookeeper.server.util.ObserverManager;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * This class implements an observable server connection. It supports two
 * types of connection events: a new connection event and a connection closed 
 * event.
 * 
 * In order to be able to receive connection notification, applications
 * are required to implement {@link ConnectionObserver} interface and register
 * its instance with {@link ObserverManager}.
 */
public class ObservableNIOServerCnxn extends NIOServerCnxn implements ObservableComponent {
    private enum ConnectionEvent {
        NEW(){
            public void dispatch(ServerCnxn c, ConnectionObserver o){
                o.onNew(c);
            }
        },
        CLOSE(){
            public void dispatch(ServerCnxn c, ConnectionObserver o){
                o.onClose(c);
            }            
        };
        public abstract void dispatch(ServerCnxn c, ConnectionObserver o);
    }

    static public class Factory extends NIOServerCnxn.Factory{
        @Override
        protected NIOServerCnxn createConnection(int clientId) throws IOException {
            return new ObservableNIOServerCnxn(zks,gcp,clientId,this);
        }

        public Factory(int port) throws IOException {
            super();
        }
    }
    
    public ObservableNIOServerCnxn(ZooKeeperServer zk, GeneralCP gcp, int clientId, Factory factory) throws IOException {
        super(zk, gcp, clientId, factory);
    }

    public void close() {
        ObserverManager.getInstance().notifyObservers(this,ConnectionEvent.CLOSE);
        super.close();
    }

    public void dispatchEvent(Object observer, Object args) {
        ConnectionEvent ev=(ConnectionEvent)args;
        ev.dispatch(this, (ConnectionObserver)observer);
    }

    public void finishSessionInit(boolean valid, RequestInfo info) {
        super.finishSessionInit(valid, info);
        if(valid && !closed)
            ObserverManager.getInstance().notifyObservers(this,ConnectionEvent.NEW);
    }
}