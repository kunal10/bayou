package ut.distcomp.bayou;

import java.util.ArrayList;
import java.util.HashMap;

public class DataStore {
	public DataStore() {
		super();
		this.playlist = new HashMap<>();
	}

	public void execute(Operation op) {
		switch (op.getOpType()) {
		case DELETE:
			delete(op.getSong());
			break;
		case PUT:
			put(op.getSong(), op.getUrl());
			break;
		// Ignore other operations as they don't modify DataStore.
		case GET:
		case CREATE:
		case RETIRE:
		default:
			break;
		}
	}

	public void undo(Operation op, WriteLog writeLog) {
		switch (op.getOpType()) {
		case DELETE:
		case PUT:
			delete(op.getSong());
			// Rerun all operations for this song till this operation.
			ArrayList<Operation> log = writeLog.getLog();
			int index = log.indexOf(op);
			for (int i = 0; i < index && i < log.size(); i++) {
				Operation opr = log.get(i);
				if (opr.getSong().equals(op.getSong())) {
					execute(opr);
				}
			}
			break;
		// Ignore other operations as they don't modify DataStore.
		case GET:
		case CREATE:
		case RETIRE:
		default:
			break;
		}
	}

	public void rollbackTo(int index, WriteLog writeLog) {
		ArrayList<Operation> log = writeLog.getLog();
		for (int i = index; i < log.size(); i++) {
			undo(log.get(i), writeLog);
		}
	}

	public void rollforwardFrom(int index, WriteLog writeLog) {
		ArrayList<Operation> log = writeLog.getLog();
		for (int i = index; i < log.size(); i++) {
			execute(log.get(i));
		}
	}

	private void delete(String song) {
		if (playlist.containsKey(song)) {
			playlist.remove(song);
		}
	}

	private void put(String song, String url) {
		playlist.put(song, url);
	}

	// NOTE : Returns err_key if song is not present.
	public String get(String song) {
		if (playlist.containsKey(song)) {
			return playlist.get(song);
		} else {
			return err_key;
		}
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("\nPlaylist : ");
		for (String song : playlist.keySet()) {
			result.append(song + playlist.get(song) + "\n");
		}
		return result.toString();
	}

	public HashMap<String, String> getPlaylist() {
		return playlist;
	}

	private HashMap<String, String> playlist;
	private static final String err_key = "ERR_KEY";
}