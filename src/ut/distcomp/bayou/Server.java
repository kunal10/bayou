package ut.distcomp.bayou;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

public class Server implements NetworkNodes {
	public Server(int serverPid) {
		this.serverPid = serverPid;
		this.queue = new LinkedBlockingQueue<>();
		this.nc = new NetController(
				new Config(serverPid, "LogServer" + serverPid), queue);
		this.dataStore = new DataStore();
		this.writeLog = new WriteLog();
		// This will be set 
		this.serverId = null;
		this.isPrimary = false;
		this.csn = -1;
		this.versionVector = new HashMap<>();
		this.pause = false;
	}

	public void joinServer() {
		// TODO(asvenk) :
		// Make nc connections to all other existing servers
		// Run creation algo and get a server ID
	}
	
	public void retireServer(){
		// TODO :
		// Run retirement protocol
	}
	
	@Override
	public void breakConnection(int i) {
		nc.breakOutgoingConnection(i);
	}

	@Override
	public void restoreConnection(int i) {
		try {
			nc.initOutgoingConn(i);
		} catch (IOException e) {
		}
	}
	
	public void Pause() {
		pause = true;
	}
	public void Start() {
		pause = false;
	}
	
	private void antiEntropy(Message m) {
		int rCsn = m.getCsn();
		if (rCsn < csn) {
			while() {
				
			}
		}
	}
	
	class ReceiveThread extends Thread {
		public void run() {
			while(true) {
				if (pause) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {}
					continue;
				}
				
			}
		}
	}

	private final int serverPid;
	private final NetController nc;
	private final LinkedBlockingQueue<Message> queue;
	private final DataStore dataStore;
	private WriteLog writeLog;
	private ServerId serverId;
	// TODO: Where should you set it for the first time
	private boolean isPrimary;
	private int csn;
	private HashMap<ServerId, Integer> versionVector;
	private boolean pause;
}
