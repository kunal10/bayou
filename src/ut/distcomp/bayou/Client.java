package ut.distcomp.bayou;

import java.io.IOException;

import ut.distcomp.bayou.Utils.TransactionType;
import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

public class Client implements NetworkNodes {

	public Client(int clientId) {
		this.sessionManager = new SessionManager();
		this.nc = new NetController(
				new Config(clientId, "LogClient" + clientId));
		this.clientId = clientId;
	}

	public void joinClient(int i) {
		try {
			nc.initOutgoingConn(i);
			serverId = i;
		} catch (IOException e) {
			nc.getConfig().logger.severe(
					"Could not initialze outgoing connection to server " + i);
		}
	}

	public void put(String songName, String url) {
		sessionManager.ExecuteTransaction(TransactionType.PUT, songName, url);
	}

	public void get(String songName) {
		String result = sessionManager.ExecuteTransaction(TransactionType.GET,
				songName, "");
		System.out.println(result);
	}

	public void delete(String songName) {
		sessionManager.ExecuteTransaction(TransactionType.DELETE, songName, "");
	}

	@Override
	public void breakConnection(int i) {
		nc.breakOutgoingConnection(i);
	}

	@Override
	public void restoreConnection(int i) {
		joinClient(i);
	}

	private int serverId;
	private final int clientId;
	private final SessionManager sessionManager;
	private final NetController nc;
}
