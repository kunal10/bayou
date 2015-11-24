package ut.distcomp.bayou;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import ut.distcomp.bayou.Message.NodeType;
import ut.distcomp.bayou.Operation.OperationType;
import ut.distcomp.framework.NetController;

public class SessionManager {

	public SessionManager(int clientId, int serverProcId, NetController nc,
			BlockingQueue<Message> queue) {
		readVector = new HashMap<>();
		writeVector = new HashMap<>();
		this.clientId = clientId;
		this.serverProcId = serverProcId;
		this.nc = nc;
		this.logger = nc.getConfig().logger;
		this.queue = queue;
	}

	// TODO(asvenk) : Don't you need to send a STATE_REQ to the server to whom
	// you are sending the read/write.
	public String ExecuteTransaction(OperationType op, String songName,
			String url) {
		HashMap<ServerId, Integer> serverVector = null;
		if (op == OperationType.GET) {
			return Read(serverVector, songName);
		} else {
			Write(serverVector, songName, url, op);
		}
		return null;
	}

	private String Read(HashMap<ServerId, Integer> serverVector,
			String songName) {
		String result = "";
		if (guarantee(serverVector)) {
			// Send a message to read to the server
			sendReadToServer(songName);
			// Extract the result and the server vector
			Message reply = getServerReply();
			// TODO: Add check on message type
			// Server will not send null msg !! It will send msg with url null..
			// Think about this and make the appropriate change 
			// on client/server side. Currently I am sending msg with null url.
			if (reply != null) {
				// TODO: Reset the read vector to max of read and server vector
				// Should this be done only when url is null or always ??
			} else {
				logger.severe("Server: " + serverProcId
						+ "returned null when guarantees were satisfied");
				result = errorDep;
			}
		} else {
			result = errorDep;
		}
		return result;
	}

	private Message getServerReply() {
		Message reply = null;
		try {
			reply = queue.take();
		} catch (InterruptedException e) {
			logger.severe("Interrupted while waiting for a reply from server");
		}
		return reply;
	}

	private void sendReadToServer(String songName) {
		Message m = new Message(clientId, serverProcId);
		m.setReadContent(songName);
		nc.sendMsg(m);
	}

	private void sendWriteToServer(String songName, String url,
			OperationType op) {
		Message m = new Message(clientId, serverProcId);
		m.setWriteContent(NodeType.CLIENT, op, songName, url, null);
		nc.sendMsg(m);
	}

	private void Write(HashMap<ServerId, Integer> serverVector, String songName,
			String url, OperationType op) {
		if (guarantee(serverVector)) {
			// Send a write to the server
			sendWriteToServer(songName, url, op);
			// Wait for the server to return the WID
			Message reply = getServerReply();
			// TODO: Check on message type
			if (reply != null) {
				// TODO: Add WID to write vector
			}
		}
	}

	private boolean guarantee(HashMap<ServerId, Integer> serverVector) {
		// Check whether S dominates the read vector
		if (!dominates(serverVector, readVector)) {
			return false;
		}
		if (!dominates(serverVector, writeVector)) {
			return false;
		}
		return true;
	}

	/*
	 * Server vector is dominant to client vector if its is greater than equal
	 * to all components.
	 */
	private boolean dominates(HashMap<ServerId, Integer> serverVector,
			HashMap<ServerId, Integer> clientVector) {
		for (ServerId server : serverVector.keySet()) {
			if (clientVector.containsKey(server)) {
				if (serverVector.get(server) < clientVector.get(server)) {
					return false;
				}
			}
		}
		return true;
	}

	public int getServerProcId() {
		return serverProcId;
	}

	public void setServerProcId(int serverProcId) {
		this.serverProcId = serverProcId;
	}

	private final HashMap<ServerId, Integer> readVector;
	private final HashMap<ServerId, Integer> writeVector;
	private int clientId;
	private int serverProcId;
	private NetController nc;
	private Logger logger;
	private BlockingQueue<Message> queue;

	private static final String errorDep = "ERR_DEP";
}
