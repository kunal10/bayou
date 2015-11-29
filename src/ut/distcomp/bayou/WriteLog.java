package ut.distcomp.bayou;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class WriteLog {
	public WriteLog(AtomicBoolean isPrimary, AtomicInteger csn, Logger logger) {
		super();
		this.isPrimary = isPrimary;
		this.csn = csn;
		this.log = new ArrayList<>();
		this.logger = logger;
	}

	// Iterates over the log and returns 1st index i st:
	// log[i].getWriteId() > writeId
	public int findInsertionPoint(int startIndex, Operation op) {
		int index = startIndex;
		WriteId writeId = op.getWriteId();
		for (Operation operation : log) {
			logger.info("WRITE LOG: "+operation.getWriteId().toString());
		}
		while (index < log.size()) {
			int result = writeId.compareTo(log.get(index).getWriteId());
			if (result == WriteId.SMALLER) {
				logger.info("S WRITE LOG - Result:" + result);
				logger.info("S WRITE LOG - 1:" + writeId.toString());
				logger.info("S WRITE LOG - 2:" + log.get(index).getWriteId());
				logger.info("S WRITE LOG Index" + index);
				return index;
			} else if (result == WriteId.EQUAL) {
				logger.info("Op already present in WriteLog: " + op.toString());
			}
			index++;
		}
		return index;
	}

	public void insert(Operation op) {
		SortedSet<Operation> writeSet = new TreeSet<>();
		writeSet.add(op);
		insert(writeSet);
		logger.info("Inserting Operation into write log :" + op.toString());
	}

	public void insert(SortedSet<Operation> writeSet) {
		Iterator<Operation> it = writeSet.iterator();
		int index = 0;
		while (it.hasNext()) {
			Operation op = it.next();
			index = findInsertionPoint(index, op);
			logger.info("Insertion point in insert function : " + index);
			WriteId writeId = op.getWriteId();
			int oldIndex = indexOf(writeId);
			if (oldIndex == -1) {
				if (isPrimary.get() && !writeId.isCommitted()) {
					writeId.setCsn(csn.incrementAndGet());
				}
				logger.info("INSERT new: " + index + ":" + op.toString());
				log.add(index, op);
			} else {
				WriteId oldWriteId = log.get(oldIndex).getWriteId();
				// Ignore the write if old write is already committed or its
				// already present.
				if (oldWriteId.isCommitted() || writeId.equals(oldWriteId)) {
					continue;
				}
				// Old write is tentative, new write is committed.
				// Remove old and add new at above determined position.
				log.remove(oldIndex);
				if (index < log.size()) {

					log.add(index, op);
					logger.info("INSERT old : " + oldIndex + "new" + index + ":"
							+ op.toString());
				} else {
					logger.info("Index: " + index);
					logger.info("Size: " + log.size());
					for (int i = 0; i < log.size(); i++) {
						logger.info(i + ": " + log.get(i).toString());
					}
					logger.info(
							"Adding at the end of the log: " + op.toString());
					log.add(op);
				}
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
		for (int i = 0; i < log.size(); i++) {
			if (writeId.isEquivalent(log.get(i).getWriteId())) {
				index = i;
				break;
			}
		}
		return index;
	}

	public void commitTentativeWrites() {
		int startIndex = csn.get() + 1;
		for (int i = startIndex; i < log.size(); i++) {
			log.get(i).getWriteId().setCsn(csn.incrementAndGet());
		}

	}

	private AtomicBoolean isPrimary;
	private AtomicInteger csn;
	private ArrayList<Operation> log;
	private Logger logger;
}
