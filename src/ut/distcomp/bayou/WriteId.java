package ut.distcomp.bayou;

import java.io.Serializable;

public class WriteId implements Serializable{
	static final int POSITIVE_INFINITY = Integer.MAX_VALUE;
	static final int NEGATIVE_INFINITY = Integer.MIN_VALUE;
	public WriteId(int csn, int acceptstamp, ServerId serverId) {
		super();
		this.csn = csn;
		this.acceptstamp = acceptstamp;
		this.serverId = serverId;
	}
	
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("<" + acceptstamp + ", ");
		if (csn != POSITIVE_INFINITY) {
			result.append(csn + ", ");
		} else {
			result.append("INFINITY, ");
		}
		if (serverId != null) {
			result.append(serverId.toString());
		} else {
			result.append("null");
		}
		result.append(" >");
		return result.toString();
	}
	
	public int getAcceptstamp() {
		return acceptstamp;
	}
	public ServerId getServerId() {
		return serverId;
	}

	private int csn;
	private int acceptstamp;
	private ServerId serverId;
	
	private static final long serialVersionUID = 1L;
}
