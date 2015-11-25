package ut.distcomp.bayou;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import ut.distcomp.bayou.Message.MessageType;
import ut.distcomp.bayou.Operation.OperationType;
import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

// TODO(klad): Are you updating the version vector on receiving writes with their 
// timestamp. Then any server which receives a creation write during anti entropy 
// doesn't need any exclusive logic to be executed
// TODO(asvenk): setting up new connections when you get some message from a 
// server who is not there in your outgoing connections. 
// Should you exclude this when you receive a create message ?

public class Server implements NetworkNodes {
	public Server(int serverPid) {
		this.serverPid = serverPid;
		this.queue = new LinkedBlockingQueue<>();
		this.nc = new NetController(
				new Config(serverPid, "LogServer" + serverPid), queue);
		this.logger = nc.getConfig().logger;
		this.dataStore = new DataStore();
		this.writeLog = new WriteLog(logger);
		// This will be set
		this.serverId = null;
		this.isPrimary = false;
		this.csn = -1;
		this.acceptstamp = 0;
		this.versionVector = new HashMap<>();
		this.pause = false;
		this.connectServers = new ArrayList<Integer>();
	}

	public void JoinServer(List<Integer> availableServers) {
		if (availableServers.size() == 0) {
			// This is the first server to join the system. Set your own
			// server id to null and set yourself as primary.
			serverId = new ServerId(0, null);
			isPrimary = true;
			// TODO: Write the first entry into log.
		} else {
			// Take the first available server and initiate connection with it.
			int firstSid = availableServers.get(0);
			restoreConnection(firstSid);
			Message m = new Message(serverPid, firstSid);
			m.setCreateReqContent(serverPid);
			Message createResp = getCreateResponse();
			processCreateRes(createResp);
		}
		startThreads();
		connectToServers(availableServers);
	}

	public List<String> PrintLog() {
		List<Operation> l = writeLog.getLog();
		List<String> printLog = new ArrayList<>();
		for (Operation operation : l) {
			OperationType opType = operation.getOpType();
			if (opType == OperationType.PUT || opType == OperationType.DELETE) {
				String songName = operation.getSong();
				String url = operation.getUrl();
				boolean isCommited = operation.getWriteId().isCommitted();
				printLog.add(formatLog(opType, songName, url, isCommited));
			}
		}
		return null;
	}

	private void startThreads() {
		receiveThread = new ReceiveThread();
		atThread = new AntiEntropyThread();
		receiveThread.start();
		atThread.start();
	}

	private void stopThreads() {
		if (receiveThread != null) {
			receiveThread.stop();
		}
		if (atThread != null) {
			atThread.stop();
		}
	}

	private String formatLog(OperationType opType, String songName, String url,
			boolean isCommited) {
		String opvalue = songName + ((url != null) ? ("," + url) : "");
		String stablebool = (isCommited) ? "TRUE" : "FALSE";
		String s = opType.toString() + ":(" + opvalue + "):" + stablebool;
		return s;
	}

	private Message getCreateResponse() {
		Message m = null;
		do {
			try {
				m = queue.take();
			} catch (InterruptedException e) {
			}
		} while (m.getMsgType() != MessageType.CREATE_RES);
		return m;
	}

	private void connectToServers(List<Integer> availableServers) {
		for (int i = 1; i < availableServers.size(); i++) {
			int sid = availableServers.get(i);
			restoreConnection(sid);
			connectServers.add(sid);
		}
	}

	public void RetireServer() {
		// TODO :
		// Run retirement protocol
		// Write a retire log to your own write log.
		
		// Shutdown receive and anti entropy threads.
		stopThreads();
		queue.clear();
		Message stateResponse = getStateResponse();
		// TODO: Send anti entropy response to someone.
		Message m = new Message(serverPid, connectServers.get(0));
		// m.setAntiEntropyResContent(stateResponse.getVersionVector());
		// Send retire message to that server.
		Message retire = new Message(serverPid, connectServers.get(0));
		retire.setRetireContent(isPrimary);
		nc.shutdown();
	}
	

	private Message getStateResponse() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void breakConnection(int i) {
		nc.breakOutgoingConnection(i);
		connectServers.remove(i);
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

	private void processCreateRes(Message m) {
		// Set both the server ID and the accept stamp.
		serverId = new ServerId(m.getWriteId().getAcceptstamp(),
				m.getWriteId().getServerId());
		acceptstamp = m.getWriteId().getAcceptstamp();
	}

	class ReceiveThread extends Thread {
		public void run() {
			while (true) {
				if (pause) {
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						logger.info("Server : " + serverPid + " paused");
					}
					continue;
				}
				Message m = getNextMessage();
				if (m == null) {
					return;
				}
				processMsg(m);
			}
		}

		private Message getNextMessage() {
			try {
				Message m = queue.take();
				// If an outgoing connection to this server is not available
				// make a new connection
				if (!nc.isOutgoingAvailable(m.getSrc())) {
					restoreConnection(m.getSrc());
				}
				return m;
			} catch (InterruptedException e) {
				logger.severe("Interrupted Server: " + serverPid);
			}
			return null;
		}

		private void processMsg(Message m) {
			switch (m.getMsgType()) {
			case CREATE_REQ:
				processCreateReq(m);
				break;
			case CREATE_RES:
				processCreateRes(m);
				break;
			case RETIRE:
				processRetireReq(m);
				break;
			case STATE_REQ:
				processStateReq(m);
				break;
			case STATE_RES:
				processStateRes(m);
				break;
			case READ:
				processRead(m);
				break;
			case WRITE:
				processWrite(m, false);
				break;
			case ANTI_ENTROPY_REQ:
				processAntiEntropyReq(m);
				break;
			case ANTI_ENTROPY_RES:
				processAntiEntropyRes(m);
				break;
			default:
				break;
			}
		}

		private void processCreateReq(Message m) {
			WriteId newServerId = processWrite(m, true);
			Message createResp = new Message(serverPid, m.getSrc());
			createResp.setCreateResContent(newServerId);
			versionVector.put(new ServerId(newServerId.getAcceptstamp(),
					newServerId.getServerId()), newServerId.getAcceptstamp());
			restoreConnection(m.getSrc());
			nc.sendMsg(createResp);
		}

		private void processRetireReq(Message m) {
			if (m.isPrimary()) {
				isPrimary = true;
			}
		}

		private void processStateReq(Message m) {
			Message stateResp = new Message(serverPid, m.getSrc());
			stateResp.setStateResContent(versionVector);
			nc.sendMsg(stateResp);
		}

		private void processStateRes(Message m) {

		}

		private void processRead(Message m) {
			Message response = new Message(m.getDest(), m.getSrc());
			Operation op = m.getOp();
			response.setReadResContent(op.getSong(),
					dataStore.get(op.getSong()), versionVector);
			nc.sendMsg(response);
		}

		// TODO: remove boolean flag and use the write to own log interface.
		private WriteId processWrite(Message m, boolean isCreate) {
			// TODO(klad) : Confirm if we need to take max with system clock ?
			acceptstamp = acceptstamp + 1;
			Operation op = m.getOp();
			int commitSeqNo = WriteId.POSITIVE_INFINITY;
			if (isPrimary) {
				csn++;
				commitSeqNo = csn;
			}
			op.setWriteId(new WriteId(commitSeqNo, acceptstamp, serverId));
			SortedSet<Operation> writeSet = new TreeSet<>();
			writeSet.add(op);
			// Add write to write log.
			writeLog.insert(writeSet);
			// Update DB.
			dataStore.execute(op);
			// Send writeId back to client.
			if (!isCreate) {
				Message response = new Message(m.getDest(), m.getSrc());
				response.setWriteResContent(op.getWriteId());
				nc.sendMsg(response);
			}
			return op.getWriteId();
		}

		private void processAntiEntropyReq(Message m) {

		}

		private void processAntiEntropyRes(Message m) {
			SortedSet<Operation> writeSet = m.getWriteSet();
			if (writeSet == null) {
				return;
			}
			// Remove write which might already be present.
			// Find insertion point of 1st write not already present in writeLog
			Operation op = writeSet.first();
			int insertionPoint = writeLog.findInsertionPoint(0, op);
			dataStore.rollbackTo(insertionPoint);
			writeLog.insert(writeSet);
			// TODO:
			dataStore.rollforwardFrom(insertionPoint, writeLog);
			// Update acceptstamp
			op = writeSet.last();
			acceptstamp = Math.max(acceptstamp,
					op.getWriteId().getAcceptstamp());
		}
	}

	class AntiEntropyThread extends Thread {
		public void run() {

		}
	}

	private void antiEntropy(Message m) {
		int rCsn = m.getCsn();
		if (rCsn < csn) {
			// while() {
			// }
		}
	}

	private final int serverPid;
	private final NetController nc;
	private final Logger logger;
	private final LinkedBlockingQueue<Message> queue;
	private final DataStore dataStore;
	// TODO: Make sure your own ID is not added into this set.
	private final List<Integer> connectServers;
	private WriteLog writeLog;
	private ServerId serverId;
	// Set for first time when the first server is added to the system.
	private boolean isPrimary;
	// TODO(klad) : Do we need to make these synchronized since they might be
	// accessed from multiple threads ??
	private int csn;
	private int acceptstamp;
	private HashMap<ServerId, Integer> versionVector;
	private boolean pause;
	private ReceiveThread receiveThread;
	private AntiEntropyThread atThread;
}
