package ut.distcomp.bayou;

import java.io.Serializable;

public class Message implements Serializable {
	public enum NodeType {
		CLIENT, SERVER,
	};
	
	public enum MessageType {
		// @formatter:off
		// MessageType	SourceType		DestinationType
		READ, 			// CLIENT 		SERVER
		WRITE,			// CLIENT		SERVER
		CREATE,			// SERVER		SERVER
		RETIRE,			// SERVER		SERVER
		ANTI_ENTROPY,	// SERVER		SERVER
		// @formatter:on
	}
	
	public Message(int src, int dest, NodeType srcType, NodeType destType,
			MessageType msgType, Operation op) {
		super();
		this.src = src;
		this.dest = dest;
		this.srcType = null;
		this.destType = null;
		this.msgType = null;
		this.op = null;
	}
	
	// TODO(klad) : Implement these.
	public void setReadContent() {
		
	}
	
	public void setWriteContent() {
		
	}
	
	public void setCreateContent() {
		
	}
	
	public void setRetireContent() {
		
	}
	
	public void setAntiEntropyContent() {
		
	}
	
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("\n\nSrc: " + src);
		result.append("\nDest: " + dest);
		result.append("\nSrcType: " + srcType.name());
		result.append("\nDestType: " + destType.name());
		result.append("\nMessageType: " + msgType.name());
		result.append("\nOperation: " + op.toString());
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

	private int src;
	private int dest;
	private NodeType srcType;
	private NodeType destType;
	private MessageType msgType;
	private Operation op;
	
	private static final long serialVersionUID = 1L;
}
