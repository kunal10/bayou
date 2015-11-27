package ut.distcomp.bayou;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import ut.distcomp.bayou.Message.MessageType;
import ut.distcomp.bayou.Message.NodeType;
import ut.distcomp.bayou.Operation.OperationType;
import ut.distcomp.bayou.Operation.TransactionType;
import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

public class Server implements NetworkNodes {
	// TODO: Add your own entry to version vector ?
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
		this.acceptstamp = -1;
		this.versionVector = Collections.synchronizedMap(new HashMap<>());
		this.pause = false;
		this.availableServers = Collections.synchronizedSet(new HashSet<>());
	}

	// Interface used by master to add a server.
	public void joinServer(Set<Integer> set) {
		connectToServers(set);
		if (set.size() == 0) {
			// This is the first server to join the system. Set your own
			// server id to null and set yourself as primary.
			isPrimary = true;
			serverId = new ServerId(0, null);
			acceptstamp = 0;
			csn++;
			WriteId writeId = new WriteId(csn, acceptstamp, serverId);
			Operation op = new Operation(OperationType.CREATE,
					TransactionType.WRITE, null, null, writeId, serverPid);
			writeLog.insert(op);
			updateState(op);
			dataStore.execute(op);
		} else {
			// Take the first available server and initiate connection with it.
			int firstSid = availableServers.iterator().next();
			logger.info("Creating from " + firstSid);
			Message m = new Message(serverPid, firstSid);
			m.setCreateReqContent(serverPid);
			nc.sendMsg(m);
			Message createResp = getCreateResponse();
			processCreateRes(createResp);
		}
		logger.info("Setting Server ID : " + serverId.toString());
		startThreads();
	}

	public List<String> printLog() {
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
			receiveThread.interrupt();
		}
		if (atThread != null) {
			atThread.interrupt();
		}
		try {
			receiveThread.join();
			logger.info("Receive Thread ended");
			atThread.join();
			logger.info("AE Thread ended");
		} catch (InterruptedException e) {
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
		// Can sending server send ANTI_ENTROPY before CREATE_RES ?
		// I think it will depend on when will it establish connection to this
		// server and when it gets added to set of available servers used in
		// anti-entropy thread. What if a server hasn't seen its retirement yet
		// ? Then this might be the case.
		do {
			try {
				m = queue.take();
			} catch (InterruptedException e) {
			}
		} while (m.getMsgType() != MessageType.CREATE_RES);
		return m;
	}

	private void connectToServers(Set<Integer> availableServers) {
		for (Integer sid : availableServers) {
			restoreConnection(sid);
			addToAvailableServers(sid);
		}
	}

	public void retireServer() {
		// Run retirement protocol
		// Shutdown receive and anti entropy threads.
		logger.info("Attempting to shut down threads");
		stopThreads();
		logger.info("Successfully shut down threads");
		// Write a retire log to your own write log.
		Message retireReq = new Message(serverPid, serverPid);
		retireReq.setRetireReqContent(serverPid);
		processWrite(retireReq);
		logger.info("Processed a retire write on myself");
		// Receive anti entropy request from someone.
		Message antiEntropyReq = waitForAntiEntropyRequest();
		// Process the request and send anti-entropy response
		processAntiEntropyReq(antiEntropyReq);
		logger.info("Received Anti entropy request from "
				+ antiEntropyReq.getSrc());
		// Send retire message to that server.
		Message retire = new Message(serverPid, antiEntropyReq.getSrc());
		retire.setRetireContent(isPrimary);
		nc.sendMsg(retire);
		logger.info("Sent retire to " + antiEntropyReq.getSrc());
		nc.shutdown();
	}

	private Message waitForAntiEntropyRequest() {
		queue.clear();
		Message aeRequest = null;
		do {
			try {
				aeRequest = queue.take();
			} catch (InterruptedException e) {
			}
		} while (aeRequest.getMsgType() != MessageType.ANTI_ENTROPY_REQ);

		return aeRequest;
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

	public void pause() {
		pause = true;
	}

	public void start() {
		pause = false;
	}

	class AntiEntropyThread extends Thread {
		public void run() {
			this.setName("AE Thread " + serverPid);
			logger.info("Starting AE thread");
			while (!this.isInterrupted()) {
				if (pause) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						logger.info("Server : " + serverPid + " paused");
					}
					continue;
				}
				HashSet<Integer> availableServers2 = new HashSet<>(
						availableServers);
				Iterator<Integer> iterator = availableServers2.iterator();
				while (iterator.hasNext()) {
					int dest = iterator.next();
					Message request = new Message(serverPid, dest);
					request.setAntiEntropyReqContent(versionVector, csn);
					nc.sendMsg(request);
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						logger.info("AE thread interrupted while sleep");
						return;
					}
				}
			}
			logger.info("Closing AE thread");
		}
	}

	class ReceiveThread extends Thread {
		public void run() {
			this.setName("Receive Thread " + serverPid);
			logger.info("Starting receive thread");
			while (true && !this.isInterrupted()) {
				if (pause) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						logger.info("Server : " + serverPid + " paused");
					}
					continue;
				}
				Message m = getNextMessage();
				if (m == null) {
					return;
				}
				try {
					processMsg(m);
				} catch (Exception e) {
					logger.info("ERROR in processing " + m.toString());
					throw e;
				}
			}
			logger.info("Closing Receive thread");
		}

		private Message getNextMessage() {
			try {
				Message m = queue.take();
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
				processWrite(m);
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
			logger.info("Received a Create Request :" + m.toString());
			WriteId writeId = processWrite(m);
			SortedSet<Operation> writeSet = computeWriteSet(m);

			Message createResp = new Message(serverPid, m.getSrc());
			createResp.setCreateResContent(writeId, writeSet);
			restoreConnection(m.getSrc());
			nc.sendMsg(createResp);
		}

		private void processRetireReq(Message m) {
			logger.info("Received a retire Request :" + m.toString());
			if (m.isPrimary()) {
				logger.info("Setting myself to primary");
				isPrimary = true;
			}
		}

		private void processStateReq(Message m) {
			logger.info("Received a state Request :" + m.toString());
			Message stateResp = new Message(serverPid, m.getSrc());
			stateResp.setStateResContent(versionVector);
			logger.info(stateResp.toString());
			nc.sendMsg(stateResp);
		}

		private void processStateRes(Message m) {

		}

		private void processRead(Message m) {
			logger.info("Received a read Request :" + m.toString());
			Message response = new Message(m.getDest(), m.getSrc());
			Operation op = m.getOp();
			response.setReadResContent(op.getSong(),
					dataStore.get(op.getSong()), versionVector);
			nc.sendMsg(response);
		}
	}

	// NOTE : All this method is only called for write requests from clients
	// or create/retire writes by other servers. Writes sent as part of
	// anti-entropy are handled in processAntiEntropyRes.
	private WriteId processWrite(Message m) {
		logger.info("Executing a write Request :" + m.toString());
		// TODO(klad) : Confirm if we need to take max with system clock ?
		acceptstamp = acceptstamp + 1;
		Operation op = m.getOp();
		int commitSeqNo = WriteId.POSITIVE_INFINITY;
		if (isPrimary) {
			csn++;
			commitSeqNo = csn;
		}
		op.setWriteId(new WriteId(commitSeqNo, acceptstamp, serverId));
		// Add write to write log.
		writeLog.insert(op);
		// Update version vector and csn.
		updateState(op);
		// Update DB.
		dataStore.execute(op);
		// Send writeId back to client.
		if (m.getSrcType() == NodeType.CLIENT) {
			Message response = new Message(m.getDest(), m.getSrc());
			response.setWriteResContent(op.getWriteId());
			nc.sendMsg(response);
		}
		return op.getWriteId();
	}

	private void processAntiEntropyReq(Message m) {
		// logger.info("Received a AE Request :" + m.toString());
		Message response = new Message(m.getDest(), m.getSrc());
		response.setAntiEntropyResContent(computeWriteSet(m));
		nc.sendMsg(response);
	}

	// Assumes that m has csn and versionVector values set appropriately.
	// NOTE : If m is a CREATE_REQ then rCsn is -1 and rVV is null.
	private SortedSet<Operation> computeWriteSet(Message m) {
		int rCsn = m.getCsn();
		Map<ServerId, Integer> rVV = m.getVersionVector();
		ArrayList<Operation> log = writeLog.getLog();
		SortedSet<Operation> writeSet = new TreeSet<>();
		// Add all committed writes unknown to receiver.
		if (rCsn < csn) {
			for (int i = rCsn + 1; i <= csn; i++) {
				writeSet.add(log.get(i));
			}
		}
		// Add all tentative writes unknown to receiver.
		for (int i = csn + 1; i < log.size(); i++) {
			Operation op = log.get(i);
			WriteId wId = op.getWriteId();
			// Receiver does not know about this server.
			if (rVV == null || !rVV.containsKey(wId.getServerId())) {
				writeSet.add(op);
			} else if (rVV.get(wId.getServerId()) < wId.getAcceptstamp()) {
				writeSet.add(op);
			}
		}
		return writeSet;
	}

	private void processCreateRes(Message m) {
		logger.info("Received a create response :" + m.toString());
		WriteId writeId = m.getWriteId();
		serverId = new ServerId(writeId.getAcceptstamp(),
				writeId.getServerId());
		acceptstamp = writeId.getAcceptstamp();
		logger.info("Adding myself: Server " + serverId.toString() + " : "
				+ acceptstamp);
		versionVector.put(serverId, acceptstamp);
		// For create response, handling of writeSet is same as anti-entropy
		// response message. Although we can optimize by avoiding some steps
		// like rollback in this case this is simpler.
		processAntiEntropyRes(m);

	}

	private void processAntiEntropyRes(Message m) {
		SortedSet<Operation> writeSet = m.getWriteSet();
		if (writeSet == null || writeSet.size() == 0) {
			return;
		}
		// logger.info("Received a AE response :" + m.toString());
		// Remove write which might already be present.
		// Find insertion point of 1st write not already present in writeLog
		Operation op = writeSet.first();
		int insertionPoint = writeLog.findInsertionPoint(0, op);
		// Rollback to insertion point.
		dataStore.rollbackTo(insertionPoint, writeLog);
		// Insert unknown writes.
		writeLog.insert(writeSet);
		// Update version vector and csn.
		updateState(writeSet);
		// Execute all operations from insertion point till end of the log.
		dataStore.rollforwardFrom(insertionPoint, writeLog);
		op = writeSet.last();
		acceptstamp = Math.max(acceptstamp, op.getWriteId().getAcceptstamp());
		logger.info("Setting acceptstamp to: " + acceptstamp);
	}

	private void updateState(Operation op) {
		WriteId writeId = op.getWriteId();
		ServerId sId = writeId.getServerId();
		// Update csn. 
		if (writeId.isCommitted() && writeId.getCsn() > csn) {
			csn = writeId.getCsn();
			logger.info("Updated csn as " + csn);
		}
		// Update version vector.
		int as = writeId.getAcceptstamp();
		if (versionVector.containsKey(sId)) {
			as = Math.max(as, versionVector.get(sId));
		}
		logger.info("Changing VV : Server " + sId.toString() + " : " + as);
		versionVector.put(sId, as);
		updateAvailableServers(op);
	}

	private void updateState(SortedSet<Operation> writeSet) {
		for (Operation op : writeSet) {
			updateState(op);
		}
	}

	private void updateAvailableServers(Operation op) {
		// If a write is of type retire then remove the entry from the version
		// vector.
		// TODO: Check whether removal from version vector here is correct.
		if (op.getOpType() == OperationType.RETIRE) {
			logger.info("RETIRE op found : " + op.toString());
			if (serverId != op.getWriteId().getServerId()) {
				versionVector.remove(op.getWriteId().getServerId());
				availableServers.remove(op.getPid());
				breakConnection(op.getPid());
			}
		} else if (op.getOpType() == OperationType.CREATE) {
			logger.info("CREATE op found : " + op.toString());
			addToAvailableServers(op.getPid());
		}
	}

	private void addToAvailableServers(int i) {
		if (i != serverPid) {
			availableServers.add(i);
			if (!nc.isOutgoingAvailable(i)) {
				restoreConnection(i);
			}
		}
	}

	private final int serverPid;
	private final NetController nc;
	private final Logger logger;
	private final LinkedBlockingQueue<Message> queue;
	private final DataStore dataStore;
	private final Set<Integer> availableServers;
	private WriteLog writeLog;
	private ServerId serverId;
	private boolean isPrimary;
	// TODO(klad) : Do we need to make these synchronized since they might be
	// accessed from multiple threads ??
	private int csn;
	private int acceptstamp;
	private Map<ServerId, Integer> versionVector;
	private boolean pause;
	private ReceiveThread receiveThread;
	private AntiEntropyThread atThread;
}
