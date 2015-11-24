package ut.distcomp.bayou;

import java.util.HashMap;

public class DataStore {
	public DataStore() {
		super();
		this.playlist = new HashMap<>();
	}
	
	public void execute(Operation op) {
		switch(op.getOpType()) {
		case DELETE:
			delete(op.getSong());
			break;
		case PUT:
			put(op.getSong(), op.getUrl());
			break;
		default:
			break;
		}
	}
	
	public void undo(Operation op) {
		// TODO(klad)
	}
	
	public void rollbackTo(int index) {
		// TODO(klad)
	}
	
	public void rollforwardFrom(int index, WriteLog writeLog) {
		// TODO(klad)
	}
	
	private void delete(String song) {
		if (playlist.containsKey(song)) {
			playlist.remove(song);
		}
	}
	
	private void put(String song, String url) {
		playlist.put(song, url);
	}
	
	// NOTE : Returns null if song is not present.
	public String get(String song) {
		if (playlist.containsKey(song)) {
			return playlist.get(song);
		}
		return null;
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

	private HashMap<String,String> playlist;
}