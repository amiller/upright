package BFT.network.concurrentNet;

import BFT.util.Role;

public interface NetQueue {

	/**
	 * @param role The role from which you received work
	 * @param index The particular replica index
	 * @param work The work received from that replica
	 */
	public abstract void addWork(Role role, int index, byte[] work);

	/**
	 * @param role The role from which you want work
	 * @param index The particular replica index
	 * @return The work from the role replica of interest
	 */
	public abstract byte[] getWork(Role role, int index);

	/**
	 * @param role The role from which you want work
	 * @return The work from a replica chosen by round robin
	 * 
	 * Uses a round-robin policy to choose from amongst the
	 * replicas of a given role.
	 */
	public abstract byte[] getWorkRR(Role role);

}