package ut.distcomp.bayou;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.SortedSet;

public class WriteLog {
	public WriteLog() {
		super();
		this.log = new ArrayList<>();
	}

	// Iterates over the log and returns 1st index i st log[i] > writeId
	public int findInsertionPoint(int startIndex, WriteId writeId) {
		int index = startIndex;
		while (index < log.size()) {
			int result = writeId.compareTo(log.get(index));
			if (result == WriteId.SMALLER) {
				return index;
			} else if (index == WriteId.EQUAL) {
				// TODO(klad) : Add log severe.
			}
			index++;
		}
		return index;
	}

	public void append(WriteId wId) {
		log.add(wId);
	}

	// NOTE : This method assumes that writeSet does not contain any writeId
	// which is present in log.
	// TODO(klad) : Revisit this later and add a check if above condition may
	// not hold.
	public void insert(SortedSet<WriteId> writeSet) {
		Iterator<WriteId> it = writeSet.iterator();
		int index = 0;
		while (it.hasNext()) {
			WriteId writeId = it.next();
			index = findInsertionPoint(index, writeId);
			log.add(index, writeId);
		}
	}
	
	public WriteId get(int index) {
		if (index < log.size()) {
			return log.get(index);
		}
		return null;
	}
	
	public ArrayList<WriteId> getLog() {
		return log;
	}

	private ArrayList<WriteId> log;
}
