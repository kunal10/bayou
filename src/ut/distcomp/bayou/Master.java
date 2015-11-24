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
				/*
				 * TODO: Retire the server with the id specified. This should
				 * block until the server can tell another server of its
				 * retirement
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
				/*
				 * TODO: Block until there are enough Anti-Entropy messages for
				 * all values to propagate through the currently connected
				 * servers. In general, the time that this function blocks for
				 * should increase linearly with the number of servers in the
				 * system.
				 */
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
	}

	private static void printLog(int serverId) {
		Server s = new Server(serverId);
		List<String> l = s.PrintLog();
		for (String string : l) {
			System.out.println(string);
		}
	}

	private static void delete(int clientId, String songName) {
		Client c = (Client) clients.get(clientId);
		if (c != null) {
			c.delete(songName);
		} else {
			System.out.println("cannot find client");
		}

	}

	private static void get(int clientId, String songName) {
		Client c = (Client) clients.get(clientId);
		if (c != null) {
			c.get(songName);
		} else {
			System.out.println("cannot find client");
		}

	}

	private static void put(int clientId, String songName, String url) {
		Client c = (Client) clients.get(clientId);
		if (c != null) {
			c.put(songName, url);
		} else {
			System.out.println("cannot find client");
		}

	}

	private static void start() {
		for (NetworkNodes s : servers.values()) {
			Server s1 = ((Server) s);
			s1.Start();
		}
	}

	private static void pause() {
		for (NetworkNodes s : servers.values()) {
			Server s1 = ((Server) s);
			s1.Pause();
		}
	}

	private static void restoreConnection(int id1, int id2) {
		if (servers.containsKey(id1)) {
			servers.get(id1).restoreConnection(id2);
		} else {
			clients.get(id1).restoreConnection(id2);
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
		s.JoinServer(getList(servers.keySet()));
		servers.put(serverId, s);
	}

	public static void joinClient(int clientId, int serverId) {
		Client c = new Client(clientId);
		clients.put(clientId, c);
		c.joinClient(serverId);
		servers.get(serverId).restoreConnection(clientId);
	}

	public static List<Integer> getList(Set<Integer> set) {
		List<Integer> l = new ArrayList<>();
		for (Integer i : set) {
			l.add(i);
		}
		return l;
	}

	static HashMap<Integer, NetworkNodes> servers = new HashMap<>();
	static HashMap<Integer, NetworkNodes> clients = new HashMap<>();
}
