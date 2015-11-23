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
		// MessageType		SourceType	DestinationType
		STATE_REQ, 			// CLIENT 	SERVER
		STATE_RES, 			// SERVER 	CLIENT
		READ, 				// CLIENT 	SERVER
		READ_RES, 			// SERVER	CLIENT
		WRITE, 				// X 		SERVER 			:X in {CLIENT,SERVER}
		WRITE_RES, 			// SERVER 	CLIENT
		CREATE_REQ, 		// SERVER 	SERVER
		CREATE_RES, 		// SERVER 	SERVER
		RETIRE, 			// SERVER 	SERVER
		ANTI_ENTROPY_REQ, 	// SERVER 	SERVER
		ANTI_ENTROPY_RES,	// SERVER 	SERVER
		// @formatter:on
	}

	public Message(int src, int dest, NodeType srcType) {
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

	public void setReadResContent(String song, String url) {
		srcType = NodeType.SERVER;
		destType = NodeType.CLIENT;
		msgType = MessageType.READ_RES;
		op = new Operation(OperationType.GET, TransactionType.READ, song, url,
				null);

	}

	public void setWriteResContent(WriteId wid) {
		srcType = NodeType.SERVER;
		destType = NodeType.CLIENT;
		msgType = MessageType.WRITE_RES;
		writeId = wid;
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

	public void setAntiEntropyReqContent(HashMap<ServerId, Integer> vv,
			int commitSeqNo) {
		srcType = NodeType.SERVER;
		destType = NodeType.SERVER;
		msgType = MessageType.ANTI_ENTROPY_REQ;
		versionVector = vv;
		csn = commitSeqNo;
	}
	
	public void setAntiEntropyResContent(SortedSet<WriteId> unknownWrites) {
		srcType = NodeType.SERVER;
		destType = NodeType.SERVER;
		msgType = MessageType.ANTI_ENTROPY_RES;
		writeSet = unknownWrites;
	}

	public void setCreateReqContent() {
		srcType = NodeType.SERVER;
		destType = NodeType.SERVER;
		msgType = MessageType.CREATE_REQ;
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
			Iterator<WriteId> it = writeSet.iterator();
			while (it.hasNext()) {
				WriteId wId = it.next();
				result.append("\t" + wId.toString());
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
	
	public SortedSet<WriteId> getWriteSet() {
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
	// TODO(klad) : Confirm if we need this.
	private SortedSet<WriteId> writeSet;
	private HashMap<ServerId, Integer> versionVector;
	private int csn;
	private boolean isPrimary;

	private static final long serialVersionUID = 1L;
}
