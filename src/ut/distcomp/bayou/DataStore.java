package ut.distcomp.bayou;

import java.util.HashMap;

public class DataStore {
	public DataStore() {
		super();
		this.playlist = new HashMap<>();
	}
	
	public void delete(String song) {
		if (playlist.containsKey(song)) {
			playlist.remove(song);
		}
	}
	
	public void put(String song, String url) {
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