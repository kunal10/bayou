package ut.distcomp.bayou;

import java.io.Serializable;

public class WriteId implements Comparable<WriteId>, Serializable {
	public static final int SMALLER = -1;
	public static final int EQUAL = 0;
	public static final int GREATER = 1;
	public static final int POSITIVE_INFINITY = Integer.MAX_VALUE;
	public static final int NEGATIVE_INFINITY = Integer.MIN_VALUE;

	public WriteId(int csn, int acceptstamp, ServerId serverId) {
		super();
		this.csn = csn;
		this.acceptstamp = acceptstamp;
		this.serverId = serverId;
	}

	public boolean isEquivalent(WriteId other) {
		return this.acceptstamp == other.getAcceptstamp()
				&& this.serverId.equals(other.getServerId());
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		WriteId other = (WriteId) obj;
		return (compareTo(other) == EQUAL);
	};

	@Override
	public int compareTo(WriteId other) {
		if (this == other) {
			return EQUAL;
		}
		if (other == null) {
			return GREATER;
		}
		if (this.csn != POSITIVE_INFINITY) {
			if (this.csn < other.getCsn()) {
				return SMALLER;
			} else if (this.csn > other.getCsn()) {
				return GREATER;
			} else {
				// NOTE : This should always return equal, because by protocol
				// 2 different writes can never get same csn.
				return this.serverId.compareTo(other.getServerId());
			}
		} else if (other.getCsn() != POSITIVE_INFINITY) {
			return GREATER;
		} else { // Both have csn inifnity.
			if (this.acceptstamp < other.getAcceptstamp()) {
				return SMALLER;
			} else if (this.acceptstamp > other.getAcceptstamp()) {
				return GREATER;
			} else {
				return this.serverId.compareTo(other.getServerId());
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("< ");
		if (csn != POSITIVE_INFINITY) {
			result.append(csn + ", ");
		} else {
			result.append("INFINITY, ");
		}
		result.append(acceptstamp + ", ");
		if (serverId != null) {
			result.append(serverId.toString());
		} else {
			result.append("null");
		}
		result.append(" >");
		return result.toString();
	}
	
	public boolean isCommitted() {
		return csn < POSITIVE_INFINITY;
	}

	public int getCsn() {
		return csn;
	}

	public int getAcceptstamp() {
		return acceptstamp;
	}

	public ServerId getServerId() {
		return serverId;
	}
	
	public void setCsn(int csn) {
		this.csn = csn;
	}

	private int csn;
	private int acceptstamp;
	private ServerId serverId;

	private static final long serialVersionUID = 1L;
}
