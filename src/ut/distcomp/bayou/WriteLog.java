package ut.distcomp.bayou;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Logger;

public class WriteLog {
	public WriteLog(Logger logger) {
		super();
		this.log = new ArrayList<>();
		this.logger = logger;
	}

	// Iterates over the log and returns 1st index i st log[i] > writeId
	public int findInsertionPoint(int startIndex, Operation op) {
		int index = startIndex;
		WriteId writeId = op.getWriteId();
		while (index < log.size()) {
			int result = writeId.compareTo(log.get(index).getWriteId());
			if (result == WriteId.SMALLER) {
				return index;
			} else if (index == WriteId.EQUAL) {
				logger.severe(
						"Op already present in WriteLog: " + op.toString());
			}
			index++;
		}
		return index;
	}
	
	public void insert(Operation op) {
		SortedSet<Operation> writeSet = new TreeSet<>();
		writeSet.add(op);
		insert(writeSet);
		logger.info("Inserting Operation into write log :"+op.toString());
	}

	public void insert(SortedSet<Operation> writeSet) {
		Iterator<Operation> it = writeSet.iterator();
		int index = 0;
		while (it.hasNext()) {
			Operation op = it.next();
			index = findInsertionPoint(index, op);
			WriteId writeId = op.getWriteId();
			int oldIndex = indexOf(writeId);
			if (oldIndex == -1) {
				log.add(index, op);
			} else {
				WriteId oldWriteId = log.get(oldIndex).getWriteId();
				// Ignore the write if its already present
				if (writeId.equals(oldWriteId)) {
					continue;
				}
				// Old write is tentative, new write is committed.
				// Remove old and add new at above determined position.
				log.remove(oldIndex);
				log.add(index, op);
			}
		}
	}

	public Operation get(int index) {
		if (index < log.size()) {
			return log.get(index);
		}
		return null;
	}

	public ArrayList<Operation> getLog() {
		return log;
	}
	
	private int indexOf(WriteId writeId) {
		int index = -1;
		for (int i = 0 ; i < log.size(); i++) {
			if (writeId.isEquivalent(log.get(i).getWriteId())) {
				index = i;
				break;
			}
		}
		return index;
	}
	
	public void commitTentativeWrites() {
		
	}
	
	private ArrayList<Operation> log;
	private Logger logger;
}
