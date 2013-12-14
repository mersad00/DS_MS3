package app_kvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import common.ServerInfo;

public class KVServer {

	private static Logger logger = Logger.getRootLogger ();

	private int port;
	private ServerSocket serverSocket;
	private boolean running;

	private ServerStatuses serverStatus;
	private List < ServerInfo > metadata;
	private String serverHashCode;
	

	/**
	 * Start KV Server at given port
	 * 
	 * @param port
	 *            given port for storage server to operate
	 */
	public KVServer ( int port ) {
		this.port = port;		
	}

	public void startServer () {
		this.serverStatus = ServerStatuses.UNDER_INITIALIZATION;
		running = initializeServer ();
		if ( serverSocket != null ) {
			while ( isRunning () ) {
				try {
					Socket client = serverSocket.accept ();
					ConnectionThread connection = new ConnectionThread (
							client , this );
					new Thread ( connection ).start ();

					logger.info ( "new Connection: Connected to "
							+ client.getInetAddress ().getHostName ()
							+ " on port " + client.getPort () );
				} catch ( IOException e ) {
					logger.error ( "Error! "
							+ "Unable to establish connection. \n" , e );
				}
			}
		}
		logger.info ( "Server stopped." );
	}

	private synchronized boolean isRunning () {
		return this.running;
	}

	/**
	 * Stops the server insofar that it won't listen at the given port any more.
	 */
	public synchronized void stopServer () {
		running = false;
		try {
			serverSocket.close ();
		} catch ( IOException e ) {
			logger.error ( "Error! " + "Unable to close socket on port: "
					+ port , e );
		}
	}

	private boolean initializeServer () {
		logger.info ( "Initialize server ..." );
		try {
			serverSocket = new ServerSocket ( port );
			logger.info ( "Server listening on port: "
					+ serverSocket.getLocalPort () );
			return true;

		} catch ( IOException e ) {
			logger.error ( "Error! Cannot open server socket:" );
			if ( e instanceof BindException ) {
				logger.error ( "Port " + port + " is already bound!" );
			}
			return false;
		}
	}

	public static void main ( String args[] ) {
		try {
			new LogSetup ( "logs/server/server.log" , Level.ALL );
		} catch ( IOException e ) {
			e.printStackTrace ();
		}
		KVServer server = new KVServer ( Integer.parseInt ( args [ 0 ] ) );
		server.startServer ();
	}

	public ServerStatuses getServerStatus () {
		return this.serverStatus;
	}

	public List < ServerInfo > getMetadata () {
		return this.metadata;
	}

	public void setMetadata ( List < ServerInfo > metadata ) {
		this.metadata = metadata;
	}

	public String getHashCode () {
		return this.serverHashCode;
	}

}
