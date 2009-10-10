package BFT.order;

import BFT.messages.RequestCore;

public interface RequestCoreQueue {

	public abstract void addCleanWork(RequestCore nb);

	public abstract RequestCore getCleanRequestCoreWork();

}