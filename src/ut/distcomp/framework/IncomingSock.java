/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

package ut.distcomp.framework;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import ut.distcomp.bayou.Message;

public class IncomingSock extends Thread {
	Socket sock;
	ObjectInputStream in;
	
	Logger logger;
	

	public IncomingSock(Socket sock, ObjectInputStream inputStream,
			LinkedBlockingQueue<Message> queue, Logger logger) throws IOException {
		this.sock = sock;
		in = inputStream;
		sock.shutdownOutput();
		this.logger = logger;
		this.queue = queue;
	}

	public void run() {
		while (!shutdownSet) {
			try {
				Message msg = (Message) in.readObject();
				queue.add(msg);
			} catch (EOFException e) {
				logger.log(Level.SEVERE, "EOF Exception");
				cleanShutdown();
			} catch (IOException e) {
				try {
					in.close();
				} catch (IOException e1) {
					logger.severe(e1.getMessage());
				}
				logger.severe(e.getMessage());
			} catch (ClassNotFoundException e) {
				try {
					in.close();
				} catch (IOException e1) {
					logger.severe(e1.getMessage());
				}
				logger.severe(e.getMessage());
			}
		}

		shutdown();
	}

	public void cleanShutdown() {
		shutdownSet = true;
	}

	protected void shutdown() {
		try {
			in.close();
		} catch (IOException e) {
		}
		try {
			sock.shutdownInput();
			sock.close();
		} catch (IOException e) {
		}
	}
	
	private LinkedBlockingQueue<Message> queue;
	private volatile boolean shutdownSet;
}
