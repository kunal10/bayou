package ut.distcomp.bayou;

import java.io.IOException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

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
	}

	public void joinServer() {
		// TODO(asvenk) :
		// Make nc connections to all other existing servers
		// Run creation algo and get a server ID
	}

	public void retireServer() {
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
				return queue.take();
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

		}

		private void processCreateRes(Message m) {

		}

		private void processRetireReq(Message m) {

		}

		private void processStateReq(Message m) {

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

		private void processWrite(Message m) {
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
			Message response = new Message(m.getDest(), m.getSrc());
			response.setWriteResContent(op.getWriteId());
			nc.sendMsg(response);
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
			dataStore.rollforwardFrom(insertionPoint, writeLog);
			// Update acceptstamp
			op = writeSet.last();
			acceptstamp = Math.max(acceptstamp,
					op.getWriteId().getAcceptstamp());
		}
	}

	private void antiEntropy(Message m) {
		int rCsn = m.getCsn();
		if (rCsn < csn) {
//			while() {
//			}
		}
	}

	private final int serverPid;
	private final NetController nc;
	private final Logger logger;
	private final LinkedBlockingQueue<Message> queue;
	private final DataStore dataStore;
	private WriteLog writeLog;
	private ServerId serverId;
	// TODO: Where should you set it for the first time
	private boolean isPrimary;
	// TODO(klad) : Do we need to make these synchronized since they might be
	// accessed from multiple threads ??
	private int csn;
	private int acceptstamp;
	private HashMap<ServerId, Integer> versionVector;
	private boolean pause;
}
