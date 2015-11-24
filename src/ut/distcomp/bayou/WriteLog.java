package ut.distcomp.bayou;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.SortedSet;
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

	// NOTE : We need to check 2 things here:
	// 1. If a writeId is already present then it should not be added again.
	// 2. If a writeSet contains a commited writeId is present in writeLog in 
	// tentative state then new committed writeId is inserted in correct slot
	// and old one retained as we need to roll it back later. When we roll 
	// forward from insertion point we need to ensure that tentaive writes for
	// which there is a committed write are removed from writeLog.
	// WriteLog will be in an inconsistent state till then and should not be 
	// used anywhere.
	
	// TODO(klad) : Fix above thing.
	public void insert(SortedSet<Operation> writeSet) {
		Iterator<Operation> it = writeSet.iterator();
		int index = 0;
		while (it.hasNext()) {
			Operation op = it.next();
			index = findInsertionPoint(index, op);
			WriteId writeId = op.getWriteId();
			// If updating status of write from tentative to committed then
			// remove old entry(if present) in write log.
			if (writeId.isCommitted()) {
				removeOldWrite(writeId);
			}
			log.add(index, op);
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
	
	private void removeOldWrite(WriteId writeId) {
		int oldIndex = -1;
		for (int i = 0 ; i < log.size(); i++) {
			if (writeId.isEquivalent(log.get(i).getWriteId())) {
				oldIndex = i;
				break;
			}
		}
		if (oldIndex != -1) {
			log.remove(oldIndex);
		}
	}

	private ArrayList<Operation> log;
	private Logger logger;
}
