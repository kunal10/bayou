package ut.distcomp.bayou;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedSet;

import ut.distcomp.bayou.Operation.OperationType;
import ut.distcomp.bayou.Operation.TransactionType;

public class Message implements Serializable {
	public enum NodeType {
		CLIENT, SERVER,
	};

	public enum MessageType {
		// @formatter:off
		// MessageType SourceType DestinationType
		STATE_REQ, // CLIENT SERVER
		STATE_RES, // SERVER CLIENT
		READ, // CLIENT SERVER
		READ_RES, // SERVER CLIENT
		WRITE, // X SERVER :X in {CLIENT,SERVER}
		WRITE_RES, // SERVER CLIENT
		CREATE_REQ, // SERVER SERVER
		CREATE_RES, // SERVER SERVER
		RETIRE, // SERVER SERVER
		ANTI_ENTROPY_REQ, // SERVER SERVER
		ANTI_ENTROPY_RES, // SERVER SERVER
		// @formatter:on
	}

	public Message(int src, int dest) {
		super();
		this.src = src;
		this.dest = dest;
		this.srcType = null;
		this.destType = null;
		this.msgType = null;
		this.op = null;
		this.writeSet = null;
		this.versionVector = null;
		this.csn = -1;
		this.isPrimary = false;
	}

	public void setStateReqContent() {
		srcType = NodeType.CLIENT;
		destType = NodeType.SERVER;
		msgType = MessageType.STATE_REQ;
	}

	public void setStateResContent(HashMap<ServerId, Integer> vv) {
		srcType = NodeType.SERVER;
		destType = NodeType.CLIENT;
		msgType = MessageType.STATE_RES;
		versionVector = vv;
	}

	public void setReadContent(String song) {
		srcType = NodeType.CLIENT;
		destType = NodeType.SERVER;
		msgType = MessageType.READ;
		op = new Operation(OperationType.GET, TransactionType.READ, song, null,
				null);
	}

	public void setReadResContent(String song, String url,
			HashMap<ServerId, Integer> vv) {
		srcType = NodeType.SERVER;
		destType = NodeType.CLIENT;
		msgType = MessageType.READ_RES;
		op = new Operation(OperationType.GET, TransactionType.READ, song, url,
				null);
		versionVector = vv;
	}

	// NOTE : url is null for delete operations
	// wId is null for CLIENT -> SERVER write messages.
	public void setWriteContent(NodeType sourceType, OperationType opType,
			String song, String url, WriteId wId) {
		srcType = sourceType;
		destType = NodeType.SERVER;
		msgType = MessageType.WRITE;
		op = new Operation(opType, TransactionType.WRITE, song, url, wId);
	}

	public void setWriteResContent(WriteId wId) {
		srcType = NodeType.SERVER;
		destType = NodeType.CLIENT;
		msgType = MessageType.WRITE_RES;
		writeId = wId;
	}

	public void setAntiEntropyReqContent(HashMap<ServerId, Integer> vv,
			int commitSeqNo) {
		srcType = NodeType.SERVER;
		destType = NodeType.SERVER;
		msgType = MessageType.ANTI_ENTROPY_REQ;
		versionVector = vv;
		csn = commitSeqNo;
	}

	public void setAntiEntropyResContent(SortedSet<Operation> unknownWrites) {
		srcType = NodeType.SERVER;
		destType = NodeType.SERVER;
		msgType = MessageType.ANTI_ENTROPY_RES;
		writeSet = unknownWrites;
	}

	// A specific type of write for creation. The only effect of this write
	// should be updating the version vector.The song field has the proc ID.
	// DB is not affected by this.
	public void setCreateReqContent(int procId) {
		srcType = NodeType.SERVER;
		destType = NodeType.SERVER;
		msgType = MessageType.CREATE_REQ;
		op = new Operation(OperationType.CREATE, TransactionType.WRITE,
				procId + "", null, null);
	}

	public void setCreateResContent(WriteId wId) {
		srcType = NodeType.SERVER;
		destType = NodeType.SERVER;
		msgType = MessageType.CREATE_RES;
		writeId = wId;
	}

	public void setRetireContent(boolean isPrimaryServer) {
		srcType = NodeType.SERVER;
		destType = NodeType.SERVER;
		msgType = MessageType.RETIRE;
		isPrimary = isPrimaryServer;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("\n\nSrc: " + src);
		result.append("\nDest: " + dest);
		result.append("\nSrcType: " + srcType.name());
		result.append("\nDestType: " + destType.name());
		result.append("\nMessageType: " + msgType.name());
		result.append("\nisPrimary: " + isPrimary);
		result.append("\nCSN: " + csn);
		if (op != null) {
			result.append("\nOperation: " + op.toString());
		}
		if (writeId != null) {
			result.append("\nWriteId: " + writeId.toString());
		}
		if (writeSet != null) {
			result.append("\nWriteSet:");
			Iterator<Operation> it = writeSet.iterator();
			while (it.hasNext()) {
				Operation op = it.next();
				result.append("\t" + op.toString());
			}
		}
		if (versionVector != null) {
			result.append("\nVersionVector:");
			for (ServerId serverId : versionVector.keySet()) {
				result.append("\tServerId:" + serverId.toString());
				result.append(", Value: " + versionVector.get(serverId));
			}
		}
		return result.toString();
	}

	public int getSrc() {
		return src;
	}

	public int getDest() {
		return dest;
	}

	public NodeType getSrcType() {
		return srcType;
	}

	public NodeType getDestType() {
		return destType;
	}

	public MessageType getMsgType() {
		return msgType;
	}

	public Operation getOp() {
		return op;
	}

	public WriteId getWriteId() {
		return writeId;
	}

	public SortedSet<Operation> getWriteSet() {
		return writeSet;
	}

	public HashMap<ServerId, Integer> getVersionVector() {
		return versionVector;
	}

	public int getCsn() {
		return csn;
	}

	public boolean isPrimary() {
		return isPrimary;
	}

	private int src;
	private int dest;
	private NodeType srcType;
	private NodeType destType;
	private MessageType msgType;
	private Operation op;
	private WriteId writeId;
	private SortedSet<Operation> writeSet;
	private HashMap<ServerId, Integer> versionVector;
	private int csn;
	private boolean isPrimary;

	private static final long serialVersionUID = 1L;
}
