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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.log4j.Logger;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.proto.WatcherEvent;

/**
 * This class manages watches. It allows watches to be associated with a string
 * and removes watchers and their watches in addition to managing triggers.
 */
public class WatchManager implements Record {
	private static final Logger LOG = Logger.getLogger(WatchManager.class);

	private LinkedHashMap<String, LinkedHashSet<Watcher>> watchTable = new LinkedHashMap<String, LinkedHashSet<Watcher>>();

	private LinkedHashMap<Watcher, LinkedHashSet<String>> watch2Paths = new LinkedHashMap<Watcher, LinkedHashSet<String>>();

	private ZooKeeperServer zks;

	public WatchManager(ZooKeeperServer zks) {
		this.zks = zks;
	}

	public synchronized int size() {
		return watchTable.size();
	}

	public synchronized void addWatch(String path, Watcher watcher) {
		LinkedHashSet<Watcher> list = watchTable.get(path);
		if (list == null) {
			list = new LinkedHashSet<Watcher>();
			watchTable.put(path, list);
		}
		list.add(watcher);

		LinkedHashSet<String> paths = watch2Paths.get(watcher);
		if (paths == null) {
			paths = new LinkedHashSet<String>();
			watch2Paths.put(watcher, paths);
		}
		paths.add(path);
	}

	public synchronized void removeWatcher(Watcher watcher) {
		HashSet<String> paths = watch2Paths.remove(watcher);
		if (paths == null) {
			return;
		}
		for (String p : paths) {
			HashSet<Watcher> list = watchTable.get(p);
			if (list != null) {
				list.remove(watcher);
				if (list.size() == 0) {
					watchTable.remove(p);
				}
			}
		}
	}

	public Set<Watcher> triggerWatch(String path, EventType type) {
		return triggerWatch(path, type, null);
	}

	public Set<Watcher> triggerWatch(String path, EventType type,
			Set<Watcher> supress) {
		WatchedEvent e = new WatchedEvent(type, KeeperState.SyncConnected, path);
		HashSet<Watcher> watchers;
		synchronized (this) {
			watchers = watchTable.remove(path);
			if (watchers == null || watchers.isEmpty()) {
				ZooTrace.logTraceMessage(LOG,
						ZooTrace.EVENT_DELIVERY_TRACE_MASK, "No watchers for "
								+ path);
				return null;
			}
			for (Watcher w : watchers) {
				HashSet<String> paths = watch2Paths.get(w);
				if (paths != null) {
					paths.remove(path);
				}
			}
		}
		for (Watcher w : watchers) {
			if (supress != null && supress.contains(w)) {
				continue;
			}
			w.process(e);
		}
		return watchers;
	}

	// added by iodine, for adding Watches in snapshot
	public void deserialize(InputArchive archive, String tag)
			throws IOException {
		archive.startRecord("watch_manager");
		int size = archive.readInt("table_size");
		System.out.println("table_size="+size);
		for (int i = 0; i < size; i++) {
			String path = archive.readString("path");
			System.out.println("path="+path);
			int watchers_size = archive.readInt("watchers_size");
			System.out.println("watchers_size="+watchers_size);
			for (int j = 0; j < watchers_size; j++) {
				int id = archive.readInt("id");
				System.out.println("id="+id);
				NIOServerCnxn watcher = zks.getServerCnxnFactory().getCnxn(id);
				if (watcher == null)
					throw new IOException("Unknown watcher id " + watcher+" for "+path);
				this.addWatch(path, watcher);
			}
		}
		archive.endRecord("watch_manager");
	}

	synchronized public void serialize(OutputArchive archive, String tag)
			throws IOException {
		archive.startRecord(this, "watch_manager");
		archive.writeInt(watchTable.size(), "table_size");
		System.out.println("table_size="+watchTable.size());
		for (Map.Entry<String, LinkedHashSet<Watcher>> entry : watchTable
				.entrySet()) {
			String path = entry.getKey();
			LinkedHashSet<Watcher> watchers = entry.getValue();
			archive.writeString(path, "path");
			System.out.println("path=" + path);
			archive.writeInt(watchers.size(), "watchers_size");
			System.out.println("watchers_size=" + watchers.size());
			int index = 0;
			for (Watcher watcher : watchers) {
				if (!(watcher instanceof NIOServerCnxn)) {
					throw new IOException("Unknown watcher " + watcher);
				}
				int clientId = ((NIOServerCnxn) watcher).getClientId();
				archive.writeInt(clientId, "id");
				System.out.println("id=" + clientId);
				index++;
			}
		}
		archive.endRecord(this, "watch_manager");
	}
}
