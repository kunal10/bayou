/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

package ut.distcomp.framework;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

import ut.distcomp.bayou.Message;

public class OutgoingSock {
	Socket sock;
	ObjectOutputStream out;

	protected OutgoingSock(Socket sock, ObjectOutputStream outputStream)
			throws IOException {
		this.sock = sock;
		out = outputStream;
		sock.shutdownInput();
	}

	/**
	 * @param msg
	 * @throws IOException
	 */
	protected synchronized void sendMsg(Message msg) throws IOException {
		out.writeObject(msg);
		out.flush();
	}

	public synchronized void cleanShutdown() {
		try {
			out.close();
		} catch (IOException e) {
		}

		try {
			sock.shutdownOutput();
			sock.close();
		} catch (IOException e) {
		}
	}
}