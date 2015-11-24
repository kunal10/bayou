package ut.distcomp.bayou;

import java.io.Serializable;

public class Operation implements Serializable{
	
	public enum OperationType {
		// TODO: Check whether this will cause a problem in any cases where we
		// are checking for operation type.
		GET, PUT, DELETE, CREATE, RETIRE,
	}

	public enum TransactionType {
		READ, WRITE,
	}

	public Operation(OperationType opType, TransactionType transactionType,
			String song, String url, WriteId writeId) {
		super();
		this.opType = opType;
		this.transactionType = transactionType;
		this.song = song;
		this.url = url;
		this.writeId = writeId;
	}

	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("\nOpType: " + opType.name());
		result.append("\nTransactionType: " + transactionType.name());
		result.append("\nSong: " + song);
		// NOTE : url can be null for delete operations.
		if (url != null) {
			result.append("\nUrl: " + url);
		}
		if (writeId != null) {
			result.append("\nWriteId: " + writeId.toString());
		}
		return result.toString();
	}

	public OperationType getOpType() {
		return opType;
	}

	public TransactionType getTransactionType() {
		return transactionType;
	}

	public String getSong() {
		return song;
	}

	public String getUrl() {
		return url;
	}

	public WriteId getWriteId() {
		return writeId;
	}

	public void setWriteId(WriteId writeId) {
		this.writeId = writeId;
	}

	private OperationType opType;
	private TransactionType transactionType;
	// Should be non-empty and non-null in all operations.
	private String song;
	// Not present in delete operations.
	private String url;
	// Present only if transactionType == WRITE
	private WriteId writeId;
	private static final long serialVersionUID = -1960600520502849058L;
	
}
