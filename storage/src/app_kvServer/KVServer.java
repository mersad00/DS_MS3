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
import java.io.OutputStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.SerializationUtil;
import utilities.LoggingManager;
import common.ServerInfo;

public class KVServer extends Thread {

	private static Logger logger;

	private int port;
	private ServerSocket serverSocket;
	private boolean running;

	private ServerStatuses serverStatus;
	private List<ServerInfo> metadata;
	private String serverHashCode;
	private ServerInfo thisServerInfo;

	/* added in order to handle Persistent storage */
	private DatabaseManager db;
	private DatabaseManager firstReplicaManager;
	private DatabaseManager secondReplicaManager;

	private Date firstCoordinatorLastSeen;
	private Date secondCoordinatorLastSeen;
	
	
	public Date getFirstCoordinatorLastSeen(){
		return firstCoordinatorLastSeen;
	}
	public synchronized void setFirstCoordinatorLastSeen(Date value){
		this.firstCoordinatorLastSeen = value;
	}
	public Date getSecondCoordinatorLastSeen(){
		return secondCoordinatorLastSeen;
	}
	public synchronized void setSecondCoordinatorLastSeen(Date value){
		this.secondCoordinatorLastSeen = value;
	}
	
	
	/**
	 * Start KV Server at given port
	 * 
	 * @param port
	 *            given port for storage server to operate
	 */
	public KVServer(int port, int cacheSize, String cacheStrategy) {
		this.port = port;
		/* creating persistent storage */
		db = new DatabaseManager(this.port, cacheSize, cacheStrategy, ".ser");
		firstReplicaManager = new DatabaseManager(this.port, cacheSize,
				cacheStrategy, ".rep1");
		secondReplicaManager = new DatabaseManager(this.port, cacheSize,
				cacheStrategy, "rep2");
		logger = LoggingManager.getInstance().createLogger(this.getClass());
	}

	public void run() {
		try {
			startServer();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * initialize and the starts the server on a given port and starts to loop
	 * and accepts new connections, then it generates a new
	 * <code>ConnectionThread</code> to handle this connection.
	 * 
	 * @throws IOException
	 */

	public void startServer () throws IOException{
		this.serverStatus = ServerStatuses.UNDER_INITIALIZATION;
		running = initializeServer();
		if (serverSocket != null) {	
			while (isRunning()) {
				try {
					Socket client = serverSocket.accept();
					ConnectionThread connection = new ConnectionThread(client,
							this, db, firstReplicaManager, secondReplicaManager);
					new Thread(connection).start();

					logger.info("new Connection: Connected to "
							+ client.getInetAddress().getHostName()
							+ " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! "
							+ "Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	/**
	 * check if the server is still running
	 * 
	 * @return boolean representing if the server still running
	 */
	private synchronized boolean isRunning() {
		return this.running;
	}

	/**
	 * Stops the server insofar that it won't listen at the given port any more.
	 */
	public synchronized void stopServer() {
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " + "Unable to close socket on port: " + port,
					e);
		}
	}

	void sendHeartbeatMessage(HeartbeatMessage msg,
			ServerInfo server) {

		logger.debug("inside send heartbeat method");
		byte[] msgBytes = SerializationUtil.toByteArray(msg);
		Socket connectionToOtherServer = null;
		OutputStream output = null;
		try {
			connectionToOtherServer = new Socket(server.getAddress(),
					server.getPort());
			output = connectionToOtherServer.getOutputStream();
			output.write(msgBytes, 0, msgBytes.length);
			output.flush();
		} catch (UnknownHostException e) {
			logger.error("Error in heartbeat data : " + server.toString()
					+ "Can not find server ");
		} catch (IOException e) {
			logger.error("Error in Error in heartbeat data : "
					+ server.toString() + "Can not make connection ");
		} finally {
			try {
				if (output != null && connectionToOtherServer != null) {
					output.close();
					connectionToOtherServer.close();
				}
			} catch (IOException e) {
				logger.error("Error in heartbeat data : " + server.toString()
						+ "Can not close connection ");
			}

		}

		logger.info("Heartbeat to '" + server.toString() + "'");
	}

	HeartbeatThread heartbeatTimer;

	private boolean initHeartbeat() {
		logger.debug("INIT HeartBeat("
				+ this.getThisServerInfo().getPort() + ") => R1["
				+ this.getThisServerInfo().getFirstReplicaInfo().getPort() + "] R2["+
				this.getThisServerInfo().getSecondReplicaInfo().getPort() + "] <= C1{"
				+ this.getThisServerInfo().getFirstCoordinatorInfo().getPort()+"} C2{"
				+ this.getThisServerInfo().getSecondCoordinatorInfo().getPort()+"}");
		
		if(this.heartbeatTimer!=null) {
			heartbeatTimer.stopTicking();
		}
		this.heartbeatTimer = new HeartbeatThread(this);
		heartbeatTimer.start();
		return true;
	}

	private  boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());

			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if (e instanceof BindException) {
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	public static void main(String args[]) {
		try {
			KVServer server;
			if (args.length >= 3)
				server = new KVServer(Integer.parseInt(args[0]),
						Integer.parseInt(args[1]), args[2]);
			else
				server = new KVServer ( Integer.parseInt ( args [ 0 ] ),  10 , "FIFO" );
			
			new LogSetup ( server.getPath() + "logs/server/server" + args[ 0 ] + ".log" , Level.ALL );
			
			
			//server.startServer ();
			Thread t = new Thread(server);
			t.start();
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (t.isAlive()) {
				/*
				 * informing the ProcessInvoker of the ECS Machine that the
				 * KVServer process started successfully
				 */
				System.out.write("$SUCCESS$".getBytes());
				System.out.flush();
			} else {
				System.out.write("$ERROR$".getBytes());
				System.out.flush();
				System.exit(-1);
			}

		} catch (IOException e) {

			e.printStackTrace();

		}

	}

	/**
	 * @return <code>ServerStatuses</code> represents the status of the server
	 */
	public ServerStatuses getServerStatus() {
		return this.serverStatus;
	}

	/**
	 * set a new status to the server
	 * 
	 * @param status
	 */
	public synchronized void setServerStatus(ServerStatuses status) {
		logger.info("set server status to : \t" + status);
		this.serverStatus = status;
	}

	/**
	 * @return List of all servers info representing the metadata
	 */
	public List<ServerInfo> getMetadata() {
		return this.metadata;
	}

	/**
	 * set a new value for the metadata
	 * 
	 * @param metadata
	 */
	public synchronized void setMetadata(List<ServerInfo> metadata) {
		this.metadata = metadata;
		logger.info("metadata updated with : " + metadata.size() + " values ");
		for (ServerInfo server : metadata) {
			
			if (server.getPort() == this.port) {
				this.thisServerInfo = server;
				break;
			}
		}
		this.thisServerInfo.setCoordinators(this.thisServerInfo, metadata);
		this.initHeartbeat();
	}

	/**
	 * @return return the <code>ServerInfo</code> of this server
	 */
	public ServerInfo getThisServerInfo() {
		return this.thisServerInfo;
	}

	public String getHashCode() {
		return this.serverHashCode;
	}

	/**
	 * shutdown the server with closing all the opening resources
	 */
	public void shutdown() {
		running = false;
		try {
			logger.info("shutting down server ... ");

			this.heartbeatTimer.stopTicking();
			this.heartbeatTimer.join();
			
			logger.debug("heartbeat timer cancelled!");

			serverSocket.close();
			System.exit(0);
		} catch (Exception e) {
			logger.info("server shutdown with errors !");
			System.exit(1);
		}
	}
	
	
	/**
	 * 
	 * @return the uri of the server jar file in the Operating System
	 */
	private String getPath(){

		/* path added in order to handle invocation by a remote process through ssh
		in order to avoid Filenotfound exception when creating Log. 
		Because when this program
		is called from a remote process, the user directory will link to 
		"/home/<user>"
		 it is sufficient to delete path variable from this code when there is no
		 remote process calling this object
		*/
		String path = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
		
		/* for local invoking of the KVserver programs(no ssh call), we remove /bin to refer the path to
		project's root path*/ 
		path = path.replace("/bin", "");
		
		/* if the name of the jar file changed! this line of code must be updated
		for handling calls within ssh */
		path = path.replace("ms3-server.jar", "");
		return path;
	}

}
