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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import org.apache.zookeeper.KeeperException;

import BFT.Debug;

/**
 * This is a full featured SessionTracker. It tracks session in grouped by tick
 * interval. It always rounds up the tick interval to provide a sort of grace
 * period. Sessions are thus expired in batches made up of sessions that expire
 * in a given interval.
 */
public class SessionTrackerImpl extends Thread implements SessionTracker {
    private static final Logger LOG = Logger.getLogger(SessionTrackerImpl.class);

    HashMap<Long, Session> sessionsById = new HashMap<Long, Session>();

    ConcurrentHashMap<Long, SessionSet> sessionSets = new ConcurrentHashMap<Long, SessionSet>();

    ConcurrentHashMap<Long, SessionInfo> sessionsWithTimeout;
    long nextSessionId = 0;
    long serverId;
    long nextExpirationTime;

    int expirationInterval;

    //These two functions are for takeCP and loadCP. nextSessionId should be in snapshot.
    public long getNextSessionId() {
        return this.nextSessionId;
    }

    public void setNextSessionId(long value) {
        this.nextSessionId = value;
    }

    public static class Session {
        Session(long sessionId, long expireTime) {
            this.sessionId = sessionId;
            this.tickTime = expireTime;
        }

        long tickTime;

        long sessionId;
    }

    public static long initializeNextSession(long id, long timestamp) {
        long nextSid = 0;
        nextSid = (timestamp << 24) >> 8;
        nextSid = nextSid | (id << 56);
        return nextSid;
    }

    static class SessionSet {
        long expireTime;

        HashSet<Session> sessions = new HashSet<Session>();
    }

    SessionExpirer expirer;

    private long roundToInterval(long time) {
        // We give a one interval grace period
        return (time / expirationInterval + 1) * expirationInterval;
    }

    public SessionTrackerImpl(SessionExpirer expirer,
                              ConcurrentHashMap<Long, SessionInfo> sessionsWithTimeout, int tickTime, long sid, long timestamp) {
        super("SessionTracker");
        this.expirer = expirer;
        this.expirationInterval = tickTime;
        this.sessionsWithTimeout = sessionsWithTimeout;
        nextExpirationTime = 0;
        this.serverId = sid;
        this.nextSessionId = initializeNextSession(sid, timestamp);
        for (long id : sessionsWithTimeout.keySet()) {
            long ttt = sessionsWithTimeout.get(id).timeToTimeout;
            addSession(id, sessionsWithTimeout.get(id).timeout, ttt, timestamp, true);
            nextSessionId++;
            //Debug.println("Initialize SessionTracker, id=" + id + " ttt=" + ttt);
            SessionSet set = sessionSets.get(ttt);
            if (set == null) {
                set = new SessionSet();
                set.expireTime = ttt;

                sessionSets.put(ttt, set);
            }
            set.sessions.add(sessionsById.get(id));
        }
        //start();
    }

    volatile boolean running = true;

    volatile long currentTime;

    @Override
    synchronized public String toString() {
        StringBuffer sb = new StringBuffer("Session Sets ("
                + sessionSets.size() + "):\n");
        ArrayList<Long> keys = new ArrayList<Long>(sessionSets.keySet());
        Collections.sort(keys);
        for (long time : keys) {
            sb.append(sessionSets.get(time).sessions.size() + " expire at "
                    + new Date(time) + ":\n");
            for (Session s : sessionSets.get(time).sessions) {
                sb.append("\t" + s.sessionId + "\n");
            }
        }
        return sb.toString();
    }

    synchronized public void checkExpiration(long timestamp) {
        //Debug.println("Check Expiration t=" + timestamp);
        for (SessionSet set : sessionSets.values()) {
            if (set.expireTime < timestamp) {
            	//Debug.println("currenttime="+timestamp+" expireTime="+set.expireTime);
                sessionSets.remove(set.expireTime);
                for (Session s : set.sessions) {
                    sessionsById.remove(s.sessionId);
                    LOG.warn("Expiring session 0x"
                            + Long.toHexString(s.sessionId));
                    Debug.println("Expiring session 0x"
                            + Long.toHexString(s.sessionId));
                    expirer.expire(s.sessionId);
                }
            }
        }
    }

    /*@Override
    synchronized public void run() {
        try {
            while (running) {
                currentTime = System.currentTimeMillis();
                if (nextExpirationTime > currentTime) {
                    this.wait(nextExpirationTime - currentTime);
                    continue;
                }
                SessionSet set;
                set = sessionSets.remove(nextExpirationTime);
                if (set != null) {
                    for (Session s : set.sessions) {
                        sessionsById.remove(s.sessionId);
                        LOG.warn("Expiring session 0x"
                                + Long.toHexString(s.sessionId));
                        expirer.expire(s.sessionId);
                    }
                }
                nextExpirationTime += expirationInterval;
            }
        } catch (InterruptedException e) {
            LOG.error("FIXMSG", e);
        }
        ZooTrace.logTraceMessage(LOG, ZooTrace.getTextTraceLevel(),
                "SessionTrackerImpl exited loop!");
    }*/

    synchronized public boolean touchSession(long sessionId, int timeout, long timestamp) {
        ZooTrace.logTraceMessage(LOG,
                ZooTrace.CLIENT_PING_TRACE_MASK,
                "SessionTrackerImpl --- Touch session: 0x"
                        + Long.toHexString(sessionId) + " with timeout " + timeout);
        Session s = sessionsById.get(sessionId);
        if (s == null) {
            checkExpiration(timestamp);
            return false;
        }
        long expireTime = roundToInterval(timestamp + timeout);
        //Debug.println("touchSession, tickTime=" + s.tickTime + " expireTime=" + expireTime + " id=" + sessionId);
        if (s.tickTime >= expireTime) {
            // Nothing needs to be done
            checkExpiration(timestamp);
            return true;
        }
        SessionSet set = sessionSets.get(s.tickTime);
        if (set != null) {
            set.sessions.remove(s);
        }
        s.tickTime = expireTime;
        SessionInfo tmp = sessionsWithTimeout.remove(s.sessionId);
        sessionsWithTimeout.put(s.sessionId, new SessionInfo(tmp.timeout, expireTime));
        //Debug.println("sessionsWithTimeout=" + sessionsWithTimeout.get(s.sessionId).timeToTimeout);
        set = sessionSets.get(s.tickTime);
        if (set == null) {
            set = new SessionSet();
            set.expireTime = expireTime;
            sessionSets.put(expireTime, set);
        }
        set.sessions.add(s);
        checkExpiration(timestamp);
        return true;
    }

    synchronized public void removeSession(long sessionId) {
        Session s = sessionsById.remove(sessionId);
        sessionsWithTimeout.remove(sessionId);
        ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                "SessionTrackerImpl --- Removing session 0x"
                        + Long.toHexString(sessionId));
        if (s != null) {
            //System.out.println(sessionSets.keys().nextElement());
            sessionSets.get(s.tickTime).sessions.remove(s);
        }
    }

    public void shutdown() {
        running = false;
        ZooTrace.logTraceMessage(LOG, ZooTrace.getTextTraceLevel(),
                "Shutdown SessionTrackerImpl!");
    }


    synchronized public long createSession(int sessionTimeout, long timestamp) {
        addSession(nextSessionId, sessionTimeout, 0, timestamp);
        return nextSessionId++;
    }

    synchronized public void addSession(long id, int sessionTimeout, long ttt, long timestamp) {
        addSession(id, sessionTimeout, ttt, timestamp, false);
    }

    synchronized public void addSession(long id, int sessionTimeout, long ttt, long timestamp, boolean loadSnapshot) {
        //Debug.println("addSession " + id);
        if (!loadSnapshot && sessionsWithTimeout.get(id) == null)
            sessionsWithTimeout.put(id, new SessionInfo(sessionTimeout, ttt));
        if (sessionsById.get(id) == null) {
            Session s = new Session(id, ttt);
            sessionsById.put(id, s);
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                    "SessionTrackerImpl --- Adding session 0x"
                            + Long.toHexString(id) + " " + sessionTimeout);
        } else {
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                    "SessionTrackerImpl --- Existing session 0x"
                            + Long.toHexString(id) + " " + sessionTimeout);
        }
        if (!loadSnapshot)
            touchSession(id, sessionTimeout, timestamp);
    }

    public void checkSession(long sessionId) throws KeeperException.SessionExpiredException {
        if (sessionsById.get(sessionId) == null) {
            throw new KeeperException.SessionExpiredException();
        }
    }
}
