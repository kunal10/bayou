package ut.distcomp.bayou;

import java.io.Serializable;
import java.util.Objects;

public class ServerId implements Comparable<ServerId>, Serializable {
	public static final int SMALLER = -1;
	public static final int EQUAL = 0;
	public static final int GREATER = 1;

	public ServerId(int acceptstamp, ServerId parentId) {
		super();
		this.acceptstamp = acceptstamp;
		this.parentId = parentId;
	}

	@Override
	public int compareTo(ServerId other) {
		if (this == other) {
			return EQUAL;
		}
		if (other == null) {
			return GREATER;
		}

		// TODO(klad) : Confirm what does ordering by names mean.
		if (this.acceptstamp < other.getAcceptstamp()) {
			return SMALLER;
		} else if (this.acceptstamp > other.getAcceptstamp()) {
			return GREATER;
		} else {
			if (this.parentId == null) {
				if (other.parentId == null) {
					return EQUAL;
				}
				return SMALLER;
			}
			return this.parentId.compareTo(other.getParentId());
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		ServerId other = (ServerId) obj;
		return (compareTo(other) == EQUAL);
	};

	@Override
	public int hashCode() {
		return Objects.hash(this.acceptstamp, this.parentId);
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

	private static final long serialVersionUID = 1L;
}