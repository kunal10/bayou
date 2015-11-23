package ut.distcomp.bayou;

public class ServerId {
	public ServerId(int acceptstamp, ServerId parentId) {
		super();
		this.acceptstamp = acceptstamp;
		this.parentId = parentId;
	}
	
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("<" + acceptstamp + ", ");
		if (parentId != null) {
			result.append(parentId.toString());
		} else {
			result.append("null");
		}
		result.append(" >");
		return result.toString();
	}
	
	public int getAcceptstamp() {
		return acceptstamp;
	}
	public ServerId getParentId() {
		return parentId;
	}
	
	private int acceptstamp;
	private ServerId parentId;
}