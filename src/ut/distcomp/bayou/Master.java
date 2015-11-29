package ut.distcomp.bayou;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

public class Master {

	public static void main(String[] args) {
		Scanner scan = new Scanner(System.in);
		while (scan.hasNextLine()) {
			String[] inputLine = scan.nextLine().split(" ");
			int clientId, serverId, id1, id2;
			String songName, URL;
			System.out.println(inputLine[0]);
			switch (inputLine[0]) {
			case "joinServer":
				serverId = Integer.parseInt(inputLine[1]);
				joinServer(serverId);
				/*
				 * Start up a new server with this id and connect it to all
				 * servers
				 */
				break;
			case "retireServer":
				serverId = Integer.parseInt(inputLine[1]);
				retireServer(serverId);
				/*
				 * Retire the server with the id specified. This should block
				 * until the server can tell another server of its retirement
				 */
				break;
			case "joinClient":
				clientId = Integer.parseInt(inputLine[1]);
				serverId = Integer.parseInt(inputLine[2]);
				joinClient(clientId, serverId);
				/*
				 * Start a new client with the id specified and connect it to
				 * the server. Start on both.
				 */
				break;
			case "breakConnection":
				id1 = Integer.parseInt(inputLine[1]);
				id2 = Integer.parseInt(inputLine[2]);
				breakConnection(id1, id2);
				breakConnection(id2, id1);
				/*
				 * Break the connection between a client and a server or between
				 * two servers. Break on both sides.
				 */
				break;
			case "restoreConnection":
				id1 = Integer.parseInt(inputLine[1]);
				id2 = Integer.parseInt(inputLine[2]);
				restoreConnection(id1, id2);
				restoreConnection(id2, id1);
				/*
				 * Restore the connection between a client and a server or
				 * between two servers
				 */
				break;
			case "pause":
				pause();
				/*
				 * Pause the system and don't allow any Anti-Entropy messages to
				 * propagate through the system
				 */
				break;
			case "start":
				start();
				/*
				 * Resume the system and allow any Anti-Entropy messages to
				 * propagate through the system
				 */
				break;
			case "stabilize":
				try {
					Thread.sleep(2 * antiEntropyDelay
							* (servers.size() - retiredServers.size()));
				} catch (InterruptedException e) {
				}
				break;
			case "printLog":
				serverId = Integer.parseInt(inputLine[1]);
				printLog(serverId);
				/*
				 * Print out a server's operation log in the format specified in
				 * the handout.
				 */
				break;
			case "put":
				clientId = Integer.parseInt(inputLine[1]);
				songName = inputLine[2];
				URL = inputLine[3];
				put(clientId, songName, URL);
				/*
				 * Instruct the client specified to associate the given URL with
				 * the given songName. This command should block until the
				 * client communicates with one server.
				 */
				break;
			case "get":
				clientId = Integer.parseInt(inputLine[1]);
				songName = inputLine[2];
				get(clientId, songName);
				/*
				 * Instruct the client specified to attempt to get the URL
				 * associated with the given songName. The value should then be
				 * printed to standard out of the master script in the format
				 * specified in the handout. This command should block until the
				 * client communicates with one server.
				 */
				break;
			case "delete":
				clientId = Integer.parseInt(inputLine[1]);
				songName = inputLine[2];
				delete(clientId, songName);
				/*
				 * Instruct the client to delete the given songName from the
				 * playlist. This command should block until the client
				 * communicates with one server.
				 */
				break;
			}
		}
		scan.close();
		for (NetworkNodes s : servers.values()) {
			((Server) s).stopThreads();
		}
		System.exit(0);
	}

	private static void retireServer(int serverId) {
		Server s = (Server) servers.get(serverId);
		s.retireServer();
		retiredServers.put(serverId, s);
	}

	private static void printLog(int serverId) {
		Server s = (Server) servers.get(serverId);
		List<String> l = s.printLog();
		if (l != null && l.size() != 0) {
			int i = 0;
			for (i = 0; i < l.size(); i++) {
				System.out.println(l.get(i));
			}
		}
	}

	private static void delete(int clientId, String songName) {
		Client c = (Client) clients.get(clientId);
		if (c != null) {
			c.delete(songName);
		} else {
		}

	}

	private static void get(int clientId, String songName) {
		Client c = (Client) clients.get(clientId);
		if (c != null) {
			System.out.println(c.get(songName));
		} else {
		}

	}

	private static void put(int clientId, String songName, String url) {
		Client c = (Client) clients.get(clientId);
		if (c != null) {
			c.put(songName, url);
		} else {
		}

	}

	private static void start() {
		for (NetworkNodes s : servers.values()) {
			Server s1 = ((Server) s);
			s1.start();
		}
	}

	private static void pause() {
		for (NetworkNodes s : servers.values()) {
			Server s1 = ((Server) s);
			s1.pause();
		}
	}

	private static void restoreConnection(int id1, int id2) {
		if (servers.containsKey(id1)) {
			servers.get(id1).restoreConnection(id2, !clients.containsKey(id2));
		} else {
			clients.get(id1).restoreConnection(id2, false);
		}
	}

	private static void breakConnection(int id1, int id2) {
		if (servers.containsKey(id1)) {
			servers.get(id1).breakConnection(id2);
		} else {
			clients.get(id1).breakConnection(id2);
		}
	}

	public static void joinServer(int serverId) {
		Server s = new Server(serverId);
		retiredServers.remove(serverId);
		Set<Integer> availableServers = new HashSet<>(servers.keySet());
		availableServers.removeAll(retiredServers.keySet());
		for (NetworkNodes node : servers.values()) {
			node.restoreConnection(serverId, true);
		}
		s.joinServer(availableServers);
		servers.put(serverId, s);
	}

	public static void joinClient(int clientId, int serverId) {
		Client c = new Client(clientId);
		clients.put(clientId, c);
		c.joinClient(serverId);
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
		}
		servers.get(serverId).restoreConnection(clientId, false);
	}

	static HashMap<Integer, NetworkNodes> servers = new HashMap<>();
	static HashMap<Integer, NetworkNodes> retiredServers = new HashMap<>();
	static HashMap<Integer, NetworkNodes> clients = new HashMap<>();

	static final int antiEntropyDelay = 250;
}
