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
		this.datastore = new DataStore();
		this.nc = new NetController(
				new Config(serverPid, "LogServer" + serverPid), queue);
	}

	public void joinServer(){
		// Make nc connections to all other existing servers
		// Run creation algo and get a server ID
	}
	
	public void retireServer(){
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

	private final int serverPid;
	private final NetController nc;
	private final LinkedBlockingQueue<Message> queue;
	private final DataStore datastore;
	// TODO(klad): Write Log
	// private final WriteLog writeLog;
	private ServerId serverId;
	// TODO: Where should you set it for the first time
	private boolean isPrimary;
	private int csn;
	private HashMap<ServerId, Integer> VersionVector;
	
}
