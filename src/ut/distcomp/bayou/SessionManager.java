package ut.distcomp.bayou;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

import ut.distcomp.bayou.Message.MessageType;
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
		this.queue = queue;
	}

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
		nc.getConfig().logger.info("Initiating write : " + songName);
		if (guarantee(serverVector)) {
			// Send a message to read to the server
			sendReadToServer(songName);
			// Extract the result and the server vector
			Message reply = getServerReply();
			if (reply != null && reply.getMsgType() == MessageType.READ_RES) {
				// Reset the read vector to max of read and server vector
				result = reply.getOp().getUrl();
				nc.getConfig().logger.info(
						"Extracted result " + result + " for " + songName);
				setMaximum(readVector, reply.getVersionVector());

			} else {
				nc.getConfig().logger
						.info("Error : receive null or different message from server :"
								+ reply.toString());
			}
		} else {
			result = errorDep;
		}
		return result;
	}

	private void setMaximum(HashMap<ServerId, Integer> readVector,
			HashMap<ServerId, Integer> relevantWriteVector) {
		for (ServerId s : relevantWriteVector.keySet()) {
			if (readVector.containsKey(s)) {
				int newVal = Math.max(readVector.get(s),
						relevantWriteVector.get(s));
				readVector.put(s, newVal);
			} else {
				readVector.put(s, relevantWriteVector.get(s));
			}
		}
	}

	private Message getServerReply() {
		Message reply = null;
		try {
			reply = queue.take();
			nc.getConfig().logger
					.info("Client extracted : " + reply.toString());
		} catch (InterruptedException e) {
			nc.getConfig().logger.severe(
					"Interrupted while waiting for a reply from server");
		}
		return reply;
	}

	private void sendReadToServer(String songName) {
		Message m = new Message(clientId, serverProcId, NodeType.CLIENT);
		m.setReadContent(songName);
		nc.sendMsg(m);
		nc.getConfig().logger.info("Client sending read " + m.toString());
	}

	private void sendWriteToServer(String songName, String url,
			OperationType op) {
		Message m = new Message(clientId, serverProcId, NodeType.CLIENT);
		m.setWriteContent(NodeType.CLIENT, op, songName, url, null);
		nc.sendMsg(m);
		nc.getConfig().logger.info("Client sending write " + m.toString());
	}

	private void Write(HashMap<ServerId, Integer> serverVector, String songName,
			String url, OperationType op) {
		nc.getConfig().logger.info("Initiating write : " + songName);
		if (guarantee(serverVector)) {
			// Send a write to the server
			sendWriteToServer(songName, url, op);
			// Wait for the server to return the WID
			Message reply = getServerReply();
			if (reply != null && reply.getMsgType() == MessageType.WRITE_RES) {
				writeVector.put(reply.getWriteId().getServerId(),
						reply.getWriteId().getAcceptstamp());
			} else {
				nc.getConfig().logger
						.severe("Should have received write response.");
				if (reply != null) {
					nc.getConfig().logger
							.severe("Instead received " + reply.toString());
				}
			}
		}
	}

	private boolean guarantee(HashMap<ServerId, Integer> serverVector) {
		// Check whether S dominates the read vector
		if (!dominates(serverVector, readVector)) {
			nc.getConfig().logger.info("Cannot provide read gurantee");
			return false;
		}
		if (!dominates(serverVector, writeVector)) {
			nc.getConfig().logger.info("Cannot provide write gurantee");
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
		nc.getConfig().logger.info("Comparing vectors ");
		logVector(serverVector);
		nc.getConfig().logger.info("and");
		logVector(clientVector);
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

	public void logVector(HashMap<ServerId, Integer> v) {
		for (ServerId s : v.keySet()) {
			nc.getConfig().logger.info(s.toString() + " : " + v.get(s));
		}
	}

	private final HashMap<ServerId, Integer> readVector;
	private final HashMap<ServerId, Integer> writeVector;
	private int clientId;
	private int serverProcId;
	private NetController nc;
	private BlockingQueue<Message> queue;

	private static final String errorDep = "ERR_DEP";
}
