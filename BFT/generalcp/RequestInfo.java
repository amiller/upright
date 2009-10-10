package BFT.generalcp;

import BFT.messages.NonDeterminism;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: Yang Wang
 * Date: 2009-2-7
 * Time: 18:47:23
 * To change this template use File | Settings | File Templates.
 */
public class RequestInfo implements Serializable{
    private boolean readonly = false;
    private int clientId;
    private long seqNo;
    private long requestId;
    private long time;
    private long random;
    private boolean lastReqBeforeCP = false;

    public RequestInfo(){}
    public RequestInfo(boolean readonly, int clientId, long seqNo, long requestId, long time, long random) {
        this.readonly = readonly;
        this.clientId = clientId;
        this.seqNo = seqNo;
        this.requestId = requestId;
        this.time = time;
	this.random = random;
    }

    public boolean isReadonly() { return readonly;}
    public int getClientId() { return clientId;}
    public long getSeqNo() { return seqNo;}
    public long getRequestId() { return requestId;}
    public long getTime() { return time;}
    public long getRandom() { return random;}
    public boolean isLastReqBeforeCP() { return lastReqBeforeCP;}
    public void setLastReqBeforeCP() { lastReqBeforeCP = true;}

    @Override
    public String toString() {
        return "clientId=" + clientId + " seqNo=" + seqNo + " requestId=" + requestId;
    }
}
