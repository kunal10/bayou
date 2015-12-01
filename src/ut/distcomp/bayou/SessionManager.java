package ut.distcomp.bayou;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

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
		this.logger = nc.getConfig().logger;
		this.queue = queue;
	}

	public String ExecuteTransaction(OperationType op, String songName,
			String url) {
		logger.info("Executing transaction : " + op.toString());
		Message serverState = getServerState();
		Map<ServerId, Integer> serverVector = serverState.getVersionVector();
		List<ServerId> excludeVector = serverState.getRetiredServers();
		if (op == OperationType.GET) {
			return Read(serverState, songName);
		} else {
			Write(serverState, songName, url, op);
		}
		return null;
	}

	private Message getServerState() {
		Map<ServerId, Integer> serverState = Collections
				.synchronizedMap(new HashMap<>());
		Message request = new Message(clientId, serverProcId);
		request.setStateReqContent();
		nc.sendMsg(request);
		logger.info("Request for server state");
		logger.info(request.toString());
		Message response = getServerResponse();
		logger.info("Received for server response");
		logger.info(response.toString());
		if (!(response.getMsgType() == MessageType.STATE_RES)) {
			nc.getConfig().logger
					.severe("Received unexpected message instead of State Response:"
							+ response.toString());

		} else {
			serverState = response.getVersionVector();
		}
		return response;
	}

	private String Read(Message serverState, String songName) {
		String result = "";
		nc.getConfig().logger.info("Initiating read : " + songName);
		if (guarantee(serverState)) {
			// Send a message to read to the server
			sendReadToServer(songName);
			// Extract the result and the server vector
			Message reply = getServerResponse();
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

	private void setMaximum(Map<ServerId, Integer> readVector,
			Map<ServerId, Integer> relevantWriteVector) {
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

	private Message getServerResponse() {
		Message response = null;
		try {
			response = queue.take();
		} catch (InterruptedException e) {
			logger.severe("Interrupted while waiting for a reply from server");
		}
		return response;
	}

	private void sendReadToServer(String songName) {
		Message m = new Message(clientId, serverProcId);
		m.setReadContent(songName);
		nc.sendMsg(m);
		nc.getConfig().logger.info("Client sending read " + m.toString());
	}

	private void sendWriteToServer(String songName, String url,
			OperationType op) {
		Message m = new Message(clientId, serverProcId);
		m.setWriteContent(NodeType.CLIENT, op, songName, url, null);
		nc.sendMsg(m);
		nc.getConfig().logger.info("Client sending write " + m.toString());
	}

	private void Write(Message serverState, String songName, String url,
			OperationType op) {
		nc.getConfig().logger.info("Initiating write : " + songName);
		if (guarantee(serverState)) {
			logger.info("Server write is guranteed");
			// Send a write to the server
			sendWriteToServer(songName, url, op);
			// Wait for the server to return the WID
			Message reply = getServerResponse();
			if (reply != null && reply.getMsgType() == MessageType.WRITE_RES) {
				writeVector.put(reply.getWriteId().getServerId(),
						reply.getWriteId().getAcceptstamp());
				logger.info("Write vector after write :");
				logVector(writeVector);
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

	private boolean guarantee(Message serverState) {
		// Check whether S dominates the read vector
		if (!dominates(serverState, readVector)) {
			nc.getConfig().logger.info("Cannot provide read gurantee");
			return false;
		}
		if (!dominates(serverState, writeVector)) {
			nc.getConfig().logger.info("Cannot provide write gurantee");
			return false;
		}
		return true;
	}

	/*
	 * Server vector is dominant to client vector if its is greater than equal
	 * to all components.
	 */
	private boolean dominates(Message serverStateResp,
			Map<ServerId, Integer> clientVector) {
		Map<ServerId, Integer> serverState = serverStateResp.getVersionVector();
		nc.getConfig().logger.info("Comparing vectors ");
		logVector(serverState);
		nc.getConfig().logger.info("and");
		logVector(clientVector);
		logger.info("Exclude vector :");
		List<ServerId> excludeVector = serverStateResp.getRetiredServers();
		for (ServerId rs : excludeVector) {
			logger.info(rs.toString());
		}
		for (ServerId server : clientVector.keySet()) {
			if (!excludeVector.contains(server)) {
				if (serverState.containsKey(server)) {
					if (serverState.get(server) < clientVector.get(server)) {
						return false;
					}
				} else {
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

	public void logVector(Map<ServerId, Integer> v) {
		for (ServerId s : v.keySet()) {
			nc.getConfig().logger.info(s.toString() + " : " + v.get(s));
		}
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
