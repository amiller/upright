package BFT.generalcp;

import BFT.serverShim.ServerShimInterface;
import BFT.order.Parameters;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class HashThread extends Thread {

	private ServerShimInterface shim;
	private SharedState state;

	private SyncHashThread syncer;

	public HashThread(ServerShimInterface shim, SharedState state) {
		this.shim = shim;
		this.state = state;
		this.syncer = new SyncHashThread();
		this.syncer.start();
	}

	private class SyncHashThread extends Thread {

		public void run() {
			while (true) {
				try {
					FinishedFileInfo info = state.getNextSyncToHash();
					System.out.println("Processing " + info.fileName);
					ArrayList<StateToken> tmp = generateToken(info.fileName,
							StateToken.SNAPSHOT, info.seqNo);
					CPToken cpToken = new CPToken();
					cpToken.getAppCPTokens().addAll(tmp);
					cpToken.setCPSeqNo(info.seqNo);
					state.syncHashDone(cpToken);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void handleLastCPToken(long seqNo) {
		System.out.println("handleLstCPToken " + seqNo);
		try {
			CPToken cpToken = state.getNextSyncHash();
			while (cpToken.getCPSeqNo() != seqNo) {
				cpToken = state.getNextSyncHash();
			}
			System.out.println("Find token " + cpToken);
			CPToken lastCPToken = state.getLastCPToken();
			if (lastCPToken.getCPSeqNo() == seqNo) {
				System.out.println("CP Token already updated");
				return;
			}
			int logCount = GeneralCP.APP_CP_INTERVAL
					/ Parameters.checkPointInterval;
			for (int i = 0; i < lastCPToken.getLogTokenSize(); i++) {
				if (lastCPToken.getLogToken(i).getSeqNo() > seqNo)
					cpToken.addLogToken(
							lastCPToken.getLogToken(i));
			}
			cpToken.setCPSeqNo(seqNo);
			state.setLastCPToken(cpToken);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run() {
		boolean firstSync = true;
		while (true) {
			try {
				FinishedFileInfo finishedFileInfo = state.getNextLogToHash();
				System.out.println("Hash for " + finishedFileInfo.fileName);
				ArrayList<StateToken> tmp = generateToken(
						finishedFileInfo.fileName, StateToken.LOG,
						finishedFileInfo.seqNo);
				if (tmp.size() > 1) {
					throw new RuntimeException("Log file too large "
							+ finishedFileInfo.fileName);
				}
				CPToken lastCPToken = state.getLastCPToken();
				for(StateToken tk:tmp)
					lastCPToken.addLogToken(tk);
				state.waitForExec(finishedFileInfo.seqNo);
				System.out.println("ReturnCP " + lastCPToken);
				shim.returnCP(lastCPToken.getBytes(), finishedFileInfo.seqNo);

				// At a specific time, we need to replace logs with snapshot
				if (finishedFileInfo.seqNo % GeneralCP.APP_CP_INTERVAL == GeneralCP.APP_CP_INTERVAL - 1) {
					if (finishedFileInfo.seqNo - lastCPToken.getCPSeqNo() == 2 * GeneralCP.APP_CP_INTERVAL)
						handleLastCPToken(finishedFileInfo.seqNo
								- GeneralCP.APP_CP_INTERVAL);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private ArrayList<StateToken> generateToken(String fileName, int type,
			long seqNo) {
		try {
			File tmp = new File(fileName);
			if (!tmp.exists())
				throw new RuntimeException("generateToken cannot find "
						+ fileName);
			String shortName = new File(fileName).getName();
			// calculate the hash of the file
			FileInputStream fis = new FileInputStream(fileName);
			ArrayList<StateToken> list = new ArrayList<StateToken>();
			if (type == StateToken.SNAPSHOT) {
				byte[] data = new byte[1048576];
				long offset = 0;

				// System.out.println("available="+fis.available());
				while (fis.available() > 0) {
					if (fis.available() < 1048576)
						data = new byte[fis.available()];
					int len = fis.read(data);
					StateToken stateToken = new StateToken(type, shortName,
							offset, len, data, seqNo);
					list.add(stateToken);
					offset += len;
				}
			} else {

				byte[] data = new byte[fis.available()];
				fis.read(data);
				StateToken stateToken = new StateToken(type, shortName, 0,
						data.length, data, seqNo);
				list.add(stateToken);
			}
			return list;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}
