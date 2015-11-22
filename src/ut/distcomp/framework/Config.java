/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

package ut.distcomp.framework;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class Config {

	/**
	 * Loads config from a file. Optionally puts in 'procNum' if in file. See
	 * sample file for syntax
	 * 
	 * @param filename
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public Config(int procNum, String logfile){
		this.procNum = procNum;
		logger = Logger.getLogger("NetFramework" + procNum);
		FileHandler fileHandler = null;
		try {
			fileHandler = new FileHandler(logfile);
		} catch (SecurityException | IOException e1) {
		}
		logger.addHandler(fileHandler);
		logger.setUseParentHandlers(false);
		fileHandler.setFormatter(new MyFormatter());
		try {
			listenAddress = InetAddress.getByName("localhost");
		} catch (UnknownHostException e) {
			listenAddress = null;
		}
		listenPort = basePort + procNum;
	}

	/**
	 * Own IP address server is running on.
	 */
	public InetAddress listenAddress;

	/**
	 * Port to start the server on.
	 */
	public final int listenPort;

	/**
	 * This hosts number (should correspond to array above). Each host should
	 * have a different number.
	 */
	public final int procNum;

	/**
	 * Logger. Mainly used for console printing, though be diverted to a file.
	 * Verbosity can be restricted by raising level to WARN
	 */
	public final Logger logger;

	public static final int basePort = 5000;

}

class MyFormatter extends Formatter {

	@Override
	public String format(LogRecord record) {
		return record.getLevel() + ":" + record.getMessage() + "\n";
	}
}
