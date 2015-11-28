/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

/**
* The sendMsg method has been modified by Navid Yaghmazadeh to fix a bug regarding to send a message to a reconnected socket.
*/

package ut.distcomp.framework;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import ut.distcomp.bayou.Message;

/**
 * Public interface for managing network connections. You should only need to
 * use this and the Config class.
 * 
 * @author ilevy
 *
 */
public class NetController {
	private final Config config;
	private final HashMap<Integer, IncomingSock> inSockets;
	private final HashMap<Integer, OutgoingSock> outSockets;
	private final ListenServer listener;

	public NetController(Config config, LinkedBlockingQueue<Message> queue) {
		this.config = config;
		inSockets = new HashMap<>();
		outSockets = new HashMap<>();
		listener = new ListenServer(config, inSockets, queue);
		listener.start();
	}

	public Config getConfig() {
		return config;
	}

	public boolean isOutgoingAvailable(int proc) {
		OutgoingSock outSock = outSockets.get(proc);
		if (outSock == null) {
			return false;
		}
		return true;
	}

	/**
	 * Establish outgoing connection to a process
	 * 
	 * @param proc
	 * @throws IOException
	 */
	public synchronized void initOutgoingConn(int proc) throws IOException {
		if (isOutgoingAvailable(proc)) {
			config.logger
					.severe("Outgoing socket for " + proc + " already exists");
		} else {
			Socket bareSocket = new Socket(getProcessAddress(proc),
					getProcessPort(proc));
			config.logger.info("OUTGOING: Initiating outgoing socket for "
					+ getProcessPort(proc));
			ObjectOutputStream outputStream = new ObjectOutputStream(
					bareSocket.getOutputStream());
			// Send your process ID to the server to which you just initiated
			// the connection.
			outputStream.writeInt(config.procNum);
			outputStream.flush();
			outSockets.put(proc, new OutgoingSock(bareSocket, outputStream));
			config.logger
					.info(String.format("Server %d: Socket to %d established",
							config.procNum, proc));
		}
	}

	public synchronized void breakOutgoingConnection(int proc) {
		OutgoingSock outSock = outSockets.get(proc);
		if (outSock != null) {
			shutDownOutgoingSocket(proc);
		} else {
			config.logger.severe(
					"Connection to " + proc + " doesn't exist to break");
		}
	}

	private int getProcessPort(int proc) {
		return Config.basePort + proc;
	}

	private InetAddress getProcessAddress(int proc) {
		InetAddress add = null;
		try {
			add = InetAddress.getByName("localhost");
		} catch (Exception e) {
			config.logger.severe("Error while getting Inet address");
		}
		return add;
	}

	/**
	 * Removes associated entry form outgoing sock.
	 * 
	 * @param process
	 */
	public void shutDownOutgoingSocket(int process) {
		OutgoingSock outgoingSock = outSockets.get(process);
		if (outgoingSock != null) {
			outgoingSock.cleanShutdown();
			outSockets.remove(process);
		}
	}

	/**
	 * Send a msg to another process. This will send a message only if an
	 * outgoing connection already exists
	 * 
	 * @param process
	 *            int specified in the config file - 0 based
	 * @param msg
	 * @return bool indicating success
	 */
	public synchronized boolean sendMsg(Message msg) {
		int process = msg.getDest();
		try {
			OutgoingSock outSocket = outSockets.get(process);
			if (outSocket != null) {
				outSocket.sendMsg(msg);
				// config.logger.info("Sent message "+msg.toString());
			} else {
				config.logger.severe(
						"There is no outgoing socket available for " + process);
				return false;
			}
		} catch (Exception e) {
			shutDownOutgoingSocket(process);
			config.logger.severe(
					String.format("Server %d: Msg to %d failed. Exception : ",
							config.procNum, process, e.getMessage()));
			return false;
		}
		return true;
	}

	/**
	 * Shuts down threads and sockets.
	 */
	public synchronized void shutdown() {
		listener.cleanShutdown();
		if (inSockets != null) {
			for (IncomingSock sock : inSockets.values())
				if (sock != null)
					sock.cleanShutdown();
		}
		if (outSockets != null) {
			for (OutgoingSock sock : outSockets.values())
				if (sock != null)
					sock.cleanShutdown();
		}
	}
}
