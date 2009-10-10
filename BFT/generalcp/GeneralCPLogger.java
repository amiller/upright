package BFT.generalcp;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.*;

public class GeneralCPLogger {

	private ArrayList<BatchInfo> logs = new ArrayList<BatchInfo>();
	private String logPath = null;
	private SharedState state;
	private LinkedBlockingQueue<ArrayList<BatchInfo>> flushRequests = new LinkedBlockingQueue<ArrayList<BatchInfo>>();

	public GeneralCPLogger(String logPath, SharedState state) {
		if (logPath.endsWith(File.separator))
			this.logPath = logPath;
		else
			this.logPath = logPath + File.separator;
		this.state = state;
		new LogWriter().start();
	}

	public void addLog(BatchInfo info) {
		this.logs.add(info);
	}

	public void clear() {
		this.logs.clear();
	}

	public void flush() {
		try {
			flushRequests.put(logs);
			logs = new ArrayList<BatchInfo>();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private class LogWriter extends Thread {
		public void run() {
			try {
				while (true) {
					ArrayList<BatchInfo> tmp = flushRequests.take();
					long seqNo = tmp.get(tmp.size() - 1).getSeqNo();
					String fileName = logPath + "log_" + seqNo;
					ObjectOutputStream oos = new ObjectOutputStream(
							new FileOutputStream(fileName));
					oos.writeInt(tmp.size());
					for (int i = 0; i < tmp.size(); i++) {
						oos.writeObject(tmp.get(i));
					}
					state.flushDone(seqNo, fileName);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public ArrayList<BatchInfo> readLog(String shortFileName) {
		try {
			ArrayList<BatchInfo> ret = new ArrayList<BatchInfo>();
			String fileName = logPath + shortFileName;
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(
					fileName));
			int size = ois.readInt();
			for (int i = 0; i < size; i++) {
				BatchInfo info = (BatchInfo) ois.readObject();
				ret.add(info);
			}
			return ret;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

}
