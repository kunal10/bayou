/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

package ut.distcomp.framework;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;

import ut.distcomp.bayou.Message;

public class ListenServer extends Thread {

	public volatile boolean killSig = false;
	int port;
	int procNum;
	final HashMap<Integer, IncomingSock> socketList;
	Config conf;
	ServerSocket serverSock;

	private void startServerSock() {
		procNum = conf.procNum;
		port = conf.listenPort;
		try {
			serverSock = new ServerSocket(port);
			conf.logger.info(String.format(
					"Server %d: Server connection established", procNum));
		} catch (IOException e) {
			String errStr = String.format(
					"Server %d: [FATAL] Can't open server port %d", procNum,
					port);
			conf.logger.log(Level.SEVERE, errStr);
			throw new Error(errStr);
		}
	}

	protected ListenServer(Config conf, HashMap<Integer, IncomingSock> sockets,
			LinkedBlockingQueue<Message> queue) {
		this.conf = conf;
		this.socketList = sockets;
		this.queue = queue;
		startServerSock();
	}

	public void run() {
		while (!killSig) {
			try {
				Socket incomingSocket = serverSock.accept();
				// The first message sent on this connection is the process ID
				// of the process which initiated this connection.
				ObjectInputStream inputStream = new ObjectInputStream(
						incomingSocket.getInputStream());
				int incomingProcId = inputStream.readInt();
				conf.logger.log(Level.INFO,
						"Accepted connection from host name : "
								+ incomingProcId);
				IncomingSock incomingSock = null;
				incomingSock = new IncomingSock(incomingSocket, inputStream,
						queue, conf.logger);

				synchronized (socketList) {
					socketList.put(incomingProcId, incomingSock);
				}
				incomingSock.start();
				conf.logger.fine(String.format(
						"Server %d: New incoming connection accepted from %s",
						procNum,
						incomingSock.sock.getInetAddress().getHostName()));
			} catch (IOException e) {
				if (!killSig) {
					conf.logger.log(Level.INFO, String.format(
							"Server %d: Incoming socket failed", procNum), e);
				}
			}
		}
	}

	protected void cleanShutdown() {
		killSig = true;
		try {
			serverSock.close();
		} catch (IOException e) {
			conf.logger.log(Level.INFO, String.format(
					"Server %d: Error closing server socket", procNum), e);
		}
	}

	private final LinkedBlockingQueue<Message> queue;
}
