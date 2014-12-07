/**
 * This class is the main node class (Server node) which is 
 * a multi-thread server, which runs a <code>ServerSocket</code>
 * listening on a specific port and accepts new connection.
 * 
 * <p> Each new connection is handled by a new <code>ConnectionThread</code>
 * and this thread handle all the required requests.
 * 
 * <p> The connection thread shares some data in this server, which is a 
 * thread-safe class.
 * 
 * @see ConnectionThread
 * @see ServerStatuses
 * @see ServerSocket
 * @see Socket
 */

package app_kvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import utilities.LoggingManager;
import common.ServerInfo;

public class KVServer {

	private static Logger logger ;
	
	private int port;
	private ServerSocket serverSocket;
	private boolean running;

	private ServerStatuses serverStatus;
	private List < ServerInfo > metadata;
	private String serverHashCode;
	private ServerInfo thisServerInfo;

	/* added in order to handle Persistent storage */
	private static DatabaseManager db;

	/**
	 * Start KV Server at given port
	 * 
	 * @param port
	 *            given port for storage server to operate
	 */
	public KVServer ( int port, int cacheSize ,String cacheStrategy ) {
		this.port = port;		
		/* creating persistent storage  */
		db = new DatabaseManager(this.port,cacheSize,cacheStrategy);
		logger = LoggingManager.getInstance ().createLogger ( this.getClass () );
	}

	/**
	 * initialize and the starts the server on a given port and 
	 * starts to loop and accepts new connections, then it generates
	 * a new <code>ConnectionThread</code> to handle this connection.
	 * 
	 * @throws IOException
	 */
	public void startServer () throws IOException{
		new LogSetup ( "logs/server/server.log" , Level.ALL );
		this.serverStatus = ServerStatuses.UNDER_INITIALIZATION;
		running = initializeServer ();
		if ( serverSocket != null ) {
			while ( isRunning () ) {
				try {
					Socket client = serverSocket.accept ();
					ConnectionThread connection = new ConnectionThread (
							client , this, this.db );
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
	
	

	/**
	 * check if the server is still running
	 * @return  boolean representing if the server still running
	 */
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
			KVServer server;
			if(args.length >=3)
				server = new KVServer ( Integer.parseInt ( args [ 0 ] ),  Integer.parseInt ( args [ 1 ] ) , args[ 2] );
			else
				server = new KVServer ( Integer.parseInt ( args [ 0 ] ),  10 , "FIFO" );
			
			
			server.startServer ();
			
			/* informing the ProcessInvoker of the ECS Machine that
			 * the KVServer process started successfully
			 */
			System.out.write("\r".getBytes());
			System.out.flush();
			
		} catch ( IOException e ) {
			
			e.printStackTrace ();
		}
		
	}
	
	

	/**
	 * @return <code>ServerStatuses</code> represents the status of the server
	 */
	public ServerStatuses getServerStatus () {
		return this.serverStatus;
	}
	
	
	
	/**
	 * set a new status to the server
	 * @param status
	 */
	public synchronized void setServerStatus(ServerStatuses status){
		logger.info ( "set server status to : \t"+ status );
		this.serverStatus = status;
	}
	
	
	/**
	 * @return List of all servers info representing the metadata
	 */
	public List < ServerInfo > getMetadata () {
		return this.metadata;
	}
	
	
	/**
	 * set a new value for the metadata
	 * @param metadata
	 */
	public synchronized void setMetadata ( List < ServerInfo > metadata ) {
		this.metadata = metadata;
		logger.info ( "metadata updated with : " + metadata.size () + " values ");
		for(ServerInfo server : metadata){
			if ( server.getPort () == this.port ){
				this.thisServerInfo = server;
			}
		}		
	}
	
	
	/**
	 * @return return the <code>ServerInfo</code> of this server
	 */
	public ServerInfo getThisServerInfo(){
		return this.thisServerInfo;
	}
		

	public String getHashCode () {
		return this.serverHashCode;
	}
	
	
	/**
	 * shutdown the server with closing all the opening resources
	 */
	public void shutdown (){
		running = false;
		try {
			logger.info ( "shutting down server ... " );
			serverSocket.close ();
			System.exit ( 0 );
		} catch ( IOException e ) {
			logger.info ( "server shutdown with errors !" );
			System.exit ( 1 );
		}
	}

}