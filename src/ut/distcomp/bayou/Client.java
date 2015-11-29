package ut.distcomp.bayou;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import ut.distcomp.bayou.Operation.OperationType;
import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

public class Client implements NetworkNodes {

	public Client(int clientId) {
		this.queue = new LinkedBlockingQueue<>();
		this.nc = new NetController(
				new Config(clientId, "LogClient" + clientId), queue);
		this.clientId = clientId;
		this.sessionManager = new SessionManager(clientId, serverId, nc, queue);
		this.logger = nc.getConfig().logger;
	}

	public void joinClient(int i) {
		try {
			nc.initOutgoingConn(i);
			serverId = i;
			sessionManager.setServerProcId(i);
		} catch (IOException e) {
			nc.getConfig().logger.severe(
					"Could not initialze outgoing connection to server " + i);
		}
	}

	public void put(String songName, String url) {
		sessionManager.ExecuteTransaction(OperationType.PUT, songName, url);

	}

	public String get(String songName) {
		String result = sessionManager.ExecuteTransaction(OperationType.GET,
				songName, "");
		return songName + ":" + result;
	}

	public void delete(String songName) {
		sessionManager.ExecuteTransaction(OperationType.DELETE, songName, "");
	}

	@Override
	public void breakConnection(int i) {
		nc.breakOutgoingConnection(i);
	}

	@Override
	public void restoreConnection(int i, boolean isServer) {
		joinClient(i);
	}
	
	public Logger getLogger() {
		return logger;
	}

	private int serverId;
	private Logger logger;
	private final int clientId;
	private final SessionManager sessionManager;
	private final NetController nc;
	private final LinkedBlockingQueue<Message> queue;
}
