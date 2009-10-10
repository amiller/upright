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

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.ZooKeeperServer.BasicDataTreeBuilder;

import BFT.Debug;
import BFT.generalcp.*;

/**
 * This class starts and runs a standalone ZooKeeperServer.
 */
public class ZooKeeperServerMain {

	private static final Logger LOG = Logger
			.getLogger(ZooKeeperServerMain.class);
	private static final String USAGE = "Usage: ZooKeeperServerMain port datadir";
	static NIOServerCnxn.Factory t;
	static ZooKeeperServer zk;

	public static NIOServerCnxn.Factory getFactory() {
		return t;
	}

	public static ZooKeeperServer getZooKeeperServer() {
		return zk;
	}

	/*
	 * Start up the ZooKeeper server.
	 * 
	 * @param args the port and data directory
	 */
	public static void main(String[] args) {
		try {
			if (args.length != 3) {
				System.out
						.println("Usage: ZooKeeperServerMain  <id> <membership> <confFile>");
				System.exit(0);
			}
			/*String[] tmp = new String[2];
			tmp[0] = args[2];
			tmp[1] = args[3];
			ServerConfig.parse(tmp);*/
		} catch (Exception e) {
			e.printStackTrace();
			LOG.fatal("Error in config", e);
			LOG.info(USAGE);
			System.exit(2);
		}
		runStandalone(new ZooKeeperServer.Factory() {
			public NIOServerCnxn.Factory createConnectionFactory()
					throws IOException {
				return new NIOServerCnxn.Factory();
			}

			public ZooKeeperServer createServer() throws IOException {
				// create a file logger url from the command line args
				ZooKeeperServer zks = new ZooKeeperServer();
				zks.setClientPort(ServerConfig.getClientPort());

				FileTxnSnapLog ftxn = new FileTxnSnapLog(new File(ServerConfig
						.getDataLogDir()), new File(ServerConfig.getDataDir()));
				zks.setTxnLogFactory(ftxn);
				return zks;
			}
		}, Integer.parseInt(args[0]),args[1],ServerConfig.getDataDir(),ServerConfig.getDataLogDir());
	}

	public static void runStandalone(ZooKeeperServer.Factory factory, int id,
			String membership, String snapDir, String logDir) {
		try {
			// Note that this thread isn't going to be doing anything else,
			// so rather than spawning another thread, we will just call
			// run() in this thread.
			ServerStats.registerAsConcrete();
			zk = factory.createServer();

			t = factory.createConnectionFactory();
			t.setZooKeeperServer(zk);
			zk.startup();
			
			ZooKeeperServer backupZK = factory.createServer();
			NIOServerCnxn.Factory t2 = factory.createConnectionFactory();
			t2.setZooKeeperServer(backupZK);
			t2.setBackup();
			backupZK.startup();

			GeneralCP gcp = new GeneralCP(id, membership, snapDir, logDir);
			t.setGeneralCP(gcp);
			t2.setGeneralCP(gcp);

			PrimaryHelperWrapper wrapper = new PrimaryHelperWrapper(t, t2);
			gcp.setupApplication(wrapper);
			/*
			 * if (zk.isRunning()) { zk.shutdown(); }
			 */
		} catch (Exception e) {
			e.printStackTrace();
			LOG.fatal("Unexpected exception", e);
		}
		// System.exit(0);
	}
}
