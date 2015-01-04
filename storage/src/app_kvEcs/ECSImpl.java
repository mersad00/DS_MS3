package app_kvEcs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import org.apache.log4j.Logger;

import app_kvServer.ConnectionThread;
import utilities.LoggingManager;
import common.Hasher;
import common.ServerInfo;

public class ECSImpl implements ECS {

	private List<ServerInfo> serverRepository;
	/**
	 * The meta data
	 */
	private List<ServerInfo> activeServers;
	private Map<ServerInfo, ServerConnection> activeConnections;
	Logger logger = LoggingManager.getInstance().createLogger(this.getClass());
	private Hasher md5Hasher;
	//private ProcessInvoker processInvoker;
	private SshInvoker processInvoker;
	private String fileName;
	private boolean localCall;
	private boolean running;
	private ServerSocket serverSocket;
	private int port = 50018;
	/**
	 * @param fileName
	 *            : the name of the configuration file
	 * @throws FileNotFoundException
	 */
	public ECSImpl(String fileName) throws FileNotFoundException {
		this.fileName = fileName;
		/* parse the server repository */
		readServerInfo(this.fileName);
		init(pickRandomValue(serverRepository.size(), false));
	}

	ECSImpl(int numberOfNodes, String fileName) throws FileNotFoundException {
		this.fileName = fileName;
		/* parse the server repository */
		readServerInfo(this.fileName);
		init(numberOfNodes);
	}

	ECSImpl(int numberOfNodes, String fileName, boolean local)
			throws FileNotFoundException {
		this.fileName = fileName;
		localCall = local;
		/* parse the server repository */
		readServerInfo(this.fileName);
		init(numberOfNodes);
	}

	public void init(int numberOfNodes) {
		this.md5Hasher = new Hasher();
		/* this.processInvoker = new ProcessInvoker (); */
		this.processInvoker = new SshCaller();
		initService(numberOfNodes);
	}

	private void readServerInfo(String fileName) throws FileNotFoundException {
		Scanner fileReader = new Scanner(new File(fileName));
		fileReader.useDelimiter("\n");
		serverRepository = new ArrayList<ServerInfo>();

		while (fileReader.hasNext()) {
			serverRepository.add(new ServerInfo(fileReader.next().trim()));
		}
		fileReader.close();

	}

	/**
	 * Generates a random number in range
	 * 
	 * @param allowZero
	 * @param size
	 *            : the range upper bound
	 * @return
	 */
	private int pickRandomValue(int size, boolean allowZero) {
		Random randomGenerator = new Random();
		int randomNumber = randomGenerator.nextInt(size);
		if (!allowZero) {
			randomNumber += 1;
		}

		logger.info("Picked " + randomNumber + " as a random number.");
		return randomNumber;
	}

	@Override
	public void initService(int numberOfNodes) {
		running = true;
		Random rand = new Random();
		int count = 0;
		ServerInfo temp;
		List<ServerInfo> serversToStart = new ArrayList<ServerInfo>();
		this.activeConnections = new HashMap<ServerInfo, ServerConnection>();
		if (this.activeServers == null)
			this.activeServers = new ArrayList<ServerInfo>();
		// choosing servers randomly
		while (count < numberOfNodes) {
			int i = rand.nextInt(serverRepository.size());
			temp = serverRepository.get(i);
			if ((!serversToStart.contains(temp))
					&& !this.activeServers.contains(temp)) {
				serversToStart.add(temp);
				count++;
			}
		}
		logger.info("ECS will launch " + numberOfNodes + " servers ");
		launchNodes(serversToStart);

		// some nodes were not started successfully!
		if (serversToStart.size() < numberOfNodes) {
			int n = numberOfNodes - serversToStart.size();
			count = 0;
			int i = 0, r;
			while (count < n && i < (serverRepository.size() - 1)) {
				temp = serverRepository.get(i);
				if ((!serversToStart.contains(temp))
						&& !this.activeServers.contains(temp)) {
					r = launchNode(temp);
					if(r == 0){
						// server started successfully
						serversToStart.add(temp);
						temp.setServerLaunched(true);
						count++;
					}
				}
				i++;
			}
			if(count < n)
				//count += serversToStart.size();
				logger.warn("Could not start all the " + numberOfNodes +
						" servers! insetead started " + 
						this.activeServers.size() + " servers");
		}
		final ECSImpl that = this;
		new Thread(new Runnable() {
			@Override
			public void run() {
				initializeServer();
				if (serverSocket != null) {
					while (running) {
						try {
							logger.debug("FAILURE: waiting for failure reports");
							Socket client = serverSocket.accept();
							ECSConnectionThread connection = new ECSConnectionThread(
									client, that);
							new Thread(connection).start();

							logger.info("FAILURE: new Connection: Connected to "
									+ client.getInetAddress().getHostName()
									+ " on port " + client.getPort());
						} catch (IOException e) {
							logger.error("FAILURE: Error! "
									+ "Unable to establish connection. \n", e);
						}
					}
				}
				logger.info("Server stopped.");
			}
		}).start();

		// calculate the metaData
		serversToStart = calculateMetaData ( activeServers );
		
		// communicate with servers and send call init
		ECSMessage initMessage = getInitMessage ( serversToStart );
		logger.info ( "Sending init signals to servers" );
		
		// create server connection
		for ( ServerInfo server : this.activeServers ) {
			ServerConnection channel = new ServerConnection ( server );
			try {
				channel.connect ();
				channel.sendMessage ( initMessage );
				activeConnections.put ( server , channel );
				//added this line to
				channel.disconnect();
			} catch ( IOException e ) {
				logger.error ( "One server node couldn't be initiated" + server );
				// this.activeServers.remove(server);
			}

		}

		logger.info ( "Active servers are launched and handed meta-data." );
		
		logger.debug("Staring Replication Operation...");
		replicationOperation();
		logger.info(" Replication operation is done");

		logger.info("ECS started " + serversToStart.toString());

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
		
	/**
	 * tell each server to replicated its data on the next two nodes!
	 * @param serversToStart
	 * @return
	 */
	
	private void replicationOperation(){
		List<ServerConnection> locked = new ArrayList<ServerConnection>();
		for(ServerInfo serverNode: activeServers){
			List<ServerInfo> replicas = getReplicas(serverNode);
			sendData(serverNode, replicas.get(0), serverNode.getFromIndex(), serverNode.getToIndex());
			sendData(serverNode, replicas.get(1), serverNode.getFromIndex(), serverNode.getToIndex());
			locked.add(activeConnections.get(serverNode));
		}
		//releasing locks
		ECSMessage releaseLock = new ECSMessage ();
		releaseLock.setActionType ( ECSCommand.RELEASE_LOCK );
		try {	
			for(ServerConnection sChannel:locked){
				sendECSCommand(sChannel, releaseLock);
			}
		logger.debug ( "All locks are released." );
		} catch ( IOException e ) {
			logger.error ( "ReLease Lock message couldn't be sent." );
		}	
	}
	
	private ECSMessage getInitMessage ( List < ServerInfo > serversToStart ) {
		ECSMessage initMessage = new ECSMessage ();
		initMessage.setActionType ( ECSCommand.INIT );
		initMessage.setMetaData ( serversToStart );
		return initMessage;
	}

	private void launchNodes ( List < ServerInfo > serversToStart ) {
		
		/* it is considered that the invoker and invoked processes are in the same folder and machine*/
		String path = System.getProperty("user.dir");
		String command = "nohup java -jar " + path + "/ms3-server.jar ";
		String arguments[] = new String [2];
		arguments[1] = "  ERROR &";
		int result;
		
		Iterator<ServerInfo> iterator = serversToStart.iterator() ;
        while(iterator.hasNext()){
            ServerInfo item = iterator.next();
            //You can remove elements while iterating.
			// if server process started 
			arguments[0] = String.valueOf(item.getPort());
			if(!localCall)
				// for ssh calls
				result = processInvoker.invokeProcess(item.getAddress(), command, arguments);
			else
				// for local calls
				result = processInvoker.localInvokeProcess(command, arguments);	
			if(result == 0){
				this.activeServers.add(item);
				item.setServerLaunched(true);
			}
			else
				iterator.remove();
		}
		
		/*for(ServerInfo server: serversToStart){
			arguments[0] = String.valueOf(server.getPort());
			result = processInvoker.invokeProcess(server.getAddress(), command, arguments);

		}*/
		
	}

	
	/**
	 * launch a single server
	 * @param serverToStart
	 * @return 0 in case of successful launch
	 */
	private int launchNode (  ServerInfo serverToStart ) {
		
		/* it is considered that the invoker and invoked processes are in the same folder and machine*/
		String path = System.getProperty("user.dir");
		String command = "nohup java -jar " + path + "/ms3-server.jar ";
		String arguments[] = new String [2];
		arguments[1] = "  ERROR &";
		int result;
		arguments[0] = String.valueOf(serverToStart.getPort());
		if(!localCall)
			// for ssh calls
			result = processInvoker.invokeProcess(serverToStart.getAddress(), command, arguments);
		else
			//for local invocations
			result = processInvoker.localInvokeProcess(command, arguments);	
		
		if(result == 0){
				this.activeServers.add(serverToStart);
				serverToStart.setServerLaunched(true);
				return 0;
        }
			else
				return -1;
	}
		
	
	
	
	private List < ServerInfo > calculateMetaData (
			List < ServerInfo > serversToStart ) {

		for ( ServerInfo server : serversToStart ) {
			// loop and the hash values
			String hashKey = md5Hasher.getHash ( server.getAddress () + ":"
					+ server.getPort () );
			// the to index is the value of the server hash
			server.setToIndex ( hashKey );
		}

		Collections.sort ( serversToStart , new Comparator < ServerInfo > () { 
					// sort hashes
					@Override
					public int compare ( ServerInfo o1 , ServerInfo o2 ) {
						return md5Hasher.compareHashes ( o1.getToIndex () ,
								o2.getToIndex () );
					}
				} );

		for ( int i = 0 ; i < serversToStart.size () ; i++ ) {
			ServerInfo server = serversToStart.get ( i );
			ServerInfo predecessor;
			if ( i == 0 ) {
				// first node is a special case.
				predecessor = serversToStart.get ( serversToStart.size () - 1 );
			} else {
				predecessor = serversToStart.get ( i - 1 );
			}
			server.setFromIndex ( predecessor.getToIndex () );
		}
		/*
		for(int i=0; i<serversToStart.size();i++){
			serversToStart.get(i).setFirstReplicaInfo(serversToStart.get((i+1) % serversToStart.size()));
			serversToStart.get(i).setSecondReplicaInfo(serversToStart.get((i+2) % serversToStart.size()));
		}*/
		for(ServerInfo s:serversToStart){
			s.setFirstCoordinatorInfo(getMasters(s).get(0));
			s.setSecondCoordinatorInfo(getMasters(s).get(1));
			s.setFirstReplicaInfo(getReplicas(s).get(0));
			s.setSecondReplicaInfo(getReplicas(s).get(1));
		}
		//this.activeServers = serversToStart;
		//logger.debug ( "Calculated metadata " + serversToStart );
		return serversToStart;
	}

	
	public Hasher getMd5Hasher () {
		return md5Hasher;
	}

	
	@Override
	public void start () {
		// communicate with servers and send call init
		ECSMessage startMessage = new ECSMessage ();
		startMessage.setActionType ( ECSCommand.START );
		for ( ServerInfo server : this.activeServers ) {
			try {
				ServerConnection channel = activeConnections.get ( server );
				// added connect
				channel.connect();
				channel.sendMessage ( startMessage );
				// added disconnect
				channel.disconnect();
			} catch ( IOException e ) {
				logger.error ( "Could not send message to server" + server
						+ e.getMessage () );
			}

		}

		logger.info ( "Active servers are started." );
	}

	@Override
	public void shutdown () {
		// communicate with servers and send call init
		ECSMessage shutDownMessage = new ECSMessage ();
		shutDownMessage.setActionType ( ECSCommand.SHUT_DOWN );
		for ( ServerInfo server : this.activeServers ) {
			try {
				ServerConnection channel = activeConnections.get ( server );
				// added this line
				channel.connect();
				channel.sendMessage ( shutDownMessage );
				//added this line
				channel.disconnect();
			} catch ( IOException e ) {
				logger.error ( "Could not send message to server" + server
						+ e.getMessage () );
			}
		}

		this.activeConnections.clear ();
		this.activeServers.clear ();
		logger.info ( "Active servers are shutdown." );

	}

	@Override
	public void stop () {
		// communicate with servers and send call init
		ECSMessage stopMessage = new ECSMessage ();
		stopMessage.setActionType ( ECSCommand.STOP );

		for ( ServerInfo server : this.activeServers ) {
			try {
				ServerConnection channel = activeConnections.get ( server );
				// added this line
				channel.connect();
				channel.sendMessage ( stopMessage );
				// added this line
				channel.disconnect();
			} catch ( IOException e ) {
				logger.error ( "Could not send message to server" + server
						+ e.getMessage () );
			}
		}
		logger.info ( "Active servers are stopped." );

	}

	/**
	 * chooses a node from a serverRepository and add it to the system
	 */
	@Override
	public void addNode () {
		int result = -1;
		int i = 0;
		ServerInfo newNode = new ServerInfo();
		
		logger.debug("$$$$$$ SYSTEM BEFORE ADDING A NODE $$$$$$");
		for(ServerInfo s: activeServers)
			logger.debug(s.getPort());
		logger.debug("$$$$$$");
		
		
		//search in serverRepository to find the servers which are
		//not active (not launched yet), and try to launch them
		while(i < (serverRepository.size() -1 )){
			newNode = serverRepository.get(i);
			if( !this.activeServers.contains(newNode)){	
				result = launchNode(newNode);
				if(result == 0){
					// server started successfully
					break;
				}
			}
			i++;
		}
		if ( newNode == null  ) {
			logger.info ( "No available node to add." );
			return;
		}else if ( result != 0  ) {
			logger.info ( "Could not add a new Server!" );
			return;
		}
		
		calculateMetaData ( activeServers );
		
		// 1.init the newNode.
		ECSMessage initMessage = getInitMessage ( this.activeServers );
		ServerConnection channel = new ServerConnection ( newNode );
		try {
			channel.connect ();
			channel.sendMessage ( initMessage );
			activeConnections.put ( newNode , channel );
			logger.info ( "The new node added" + newNode.getAddress() + ":" + newNode.getPort() );
		} catch ( IOException e ) {
			// server could not be initiated so remove it from the list!
			logger.error ( " server node couldn't be initiated" + newNode.getAddress() + ":" +newNode.getPort() +
					" Operation addNode Not Successfull");
			activeServers.remove(newNode);
			channel.disconnect();
			activeConnections.remove(channel);
			calculateMetaData(activeServers);
			return;
		}
		
		//2. start the newNode
		ECSMessage startMessage = new ECSMessage ();
		startMessage.setActionType ( ECSCommand.START );
		try {
			channel.sendMessage ( startMessage );
		} catch ( IOException e ) {
			// server could not be started so remove it from the list!
			logger.error ( "Start message couldn't be sent to " + newNode.getAddress() + 
					":" +newNode.getPort() + " Operation addNode Not Successfull");
			activeServers.remove(newNode);
			channel.disconnect();
			activeConnections.remove(newNode);
			calculateMetaData(activeServers);
			return;
		}
		
		logger.debug("$$$$$$");
		for(ServerInfo s: activeServers)
			logger.debug(s.getPort());
		logger.debug("$$$$$$");
		
		/*3. tell the sucessor to send data to the newNode
		 * tell the two new Masters to send their data for replication to the newNode
		 */
		logger.debug("<<<<< Getting data from the successor >>>>>");
		ServerInfo successor = getSuccessor ( newNode );
		List <ServerInfo> masters = getMasters(newNode);
		List <ServerConnection> locked = new ArrayList<ServerConnection>();
		List <ServerInfo> replicas = getReplicas(newNode); 
		

		// handling special case where there is just one node in the system
		if(activeServers.size() <=2 ){
			if(sendData(successor, newNode, newNode.getFromIndex(), newNode.getToIndex()) == 0){
				locked.add(activeConnections.get(successor));
				logger.debug("move data was successfull to the new Node");
				sendMetaData();
				
				//replicate data on both sides
				if(sendData(newNode, successor, newNode.getFromIndex(), newNode.getToIndex()) == 1 ||
						sendData(successor, newNode, successor.getFromIndex(), successor.getToIndex()) == 1)
					
					logger.warn("One or two of the replication operation for node " + newNode.getAddress()
							+ ":" + newNode.getPort() + "failed");
				else{
					logger.debug("move data was sent to new Node to move"
							+ " its replication to two new replica : " +
						successor.getPort()) ;

					logger.debug("data from " + successor.getPort() +
							 " is replicated in : " +
						newNode.getPort()) ;
					
					locked.add(activeConnections.get(newNode));
					
					// release the lock from the successor, new Node
					logger.debug("releaing the locks");
					ECSMessage releaseLock = new ECSMessage ();
					releaseLock.setActionType ( ECSCommand.RELEASE_LOCK );
					try {	
						for(ServerConnection sChannel:locked){
							sendECSCommand(sChannel, releaseLock);
						}
					logger.debug ( "All locks are released." );
					} catch ( IOException e ) {
						logger.error ( "ReLease Lock message couldn't be sent." );
					}
					channel.disconnect();
					return;
				}
			}
			else{
				// when move data from successor to the newNode was not successful
				// data could not be moved to the newly added Server
				logger.error("Could not move data from " + successor.getServerName() +" to " + newNode.getServerName());
				logger.error("Operation addNode Not Successfull");
				activeServers.remove(newNode);
				channel.disconnect();
				activeConnections.remove(newNode);
				calculateMetaData(activeServers);
				return;
			}
		}
		else{
			//normal cases when there is more than two nodes in the system before adding the new node
			if(sendData(successor, newNode, newNode.getFromIndex(), newNode.getToIndex()) == 0 &&
					sendData(masters.get(0), newNode, masters.get(0).getFromIndex(), masters.get(0).getToIndex()) == 0 &&
					sendData(masters.get(1), newNode, masters.get(1).getFromIndex(), masters.get(1).getToIndex()) == 0){	
				
				locked.add(activeConnections.get(successor));
				locked.add(activeConnections.get(masters.get(0)));
				locked.add(activeConnections.get(masters.get(1)));
				
				logger.debug("move data message was sent to the new masters : " +
				masters.get(0).getPort() + "   " + masters.get(1).getPort());
				
				logger.debug("<<<< removing old replicated data from the two next nodes after the "
						+ "new node >>>>>");
				
				//4.Sending metaData updates to two new replicas
				ECSMessage metaDataUpdate = new ECSMessage ();
				metaDataUpdate.setActionType ( ECSCommand.SEND_METADATA );
				metaDataUpdate.setMetaData ( activeServers );
				try {
					ServerConnection replicaChannel = new ServerConnection(replicas.get(0));
					sendECSCommand(replicaChannel, metaDataUpdate);
					replicaChannel = new ServerConnection(replicas.get(1));
					sendECSCommand(replicaChannel, metaDataUpdate);
				} catch ( IOException e ) {
					logger.error ( "Meta-data update couldn't be sent."
							+ e.getMessage () );
				}
				
				//5. sending newNode's data for replication to the its replicas
				if(	sendData(newNode, replicas.get(0), newNode.getFromIndex(), newNode.getToIndex()) == 1 ||
					sendData(newNode, replicas.get(1), newNode.getFromIndex(), newNode.getToIndex()) == 1)
						logger.warn("One or two of the replication operation for node " + newNode.getAddress()
							+ ":" + newNode.getPort() + "failed");
				else{
					logger.debug("move data was sent to new Node to move"
							+ " its replication to two new replicas : " +
						replicas.get(0).getPort() + "   " + replicas.get(1).getPort());
					locked.add(activeConnections.get(newNode));
					
					/*6.tell to the next two nodes (newNodes replicas) to delete their replicated data which they 
					 * used to store from newNode's Masters
					 */
					// when we have 3 nodes in system This is not needed!
					if(activeServers.size() > 3){
						int r = removeData(replicas.get(0), masters.get(0).getFromIndex(), masters.get(0).getToIndex());
						r += removeData(replicas.get(1), masters.get(1).getFromIndex(), masters.get(1).getToIndex());
						if(r == 0 )
							logger.debug("remove data message was sent to two new replicas : " +
									replicas.get(0).getPort() + "   " + replicas.get(1).getPort());
						else
							logger.warn("remove data message could not be sent to two new replicas : " +
									replicas.get(0).getPort() + "   " + replicas.get(1).getPort());
							
						
						logger.debug("<<<< sending new Nodes data to the new replicas >>>>");
					}
				}
				
				//7.sends MetaData to all the servers!
				sendMetaData();
				channel.disconnect();
				
				logger.debug("releaing the locks");
				//8. release the lock from the successor, new Node and masters
				ECSMessage releaseLock = new ECSMessage ();
				releaseLock.setActionType ( ECSCommand.RELEASE_LOCK );
				try {	
					for(ServerConnection sChannel:locked){
						sendECSCommand(sChannel, releaseLock);
					}
				logger.debug ( "All locks are released." );
				} catch ( IOException e ) {
					logger.error ( "ReLease Lock message couldn't be sent." );
				}
			}
			
			// when move data from successor to the newNode was not successful
			else{
				// data could not be moved to the newly added Server
				logger.error("Could not move data from " + successor.getServerName() +" to " + newNode.getServerName());
				logger.error("Operation addNode Not Successfull");
				activeServers.remove(newNode);
				channel.disconnect();
				activeConnections.remove(newNode);
				calculateMetaData(activeServers);
				return;
			}
		}
		}


	
	
	private ServerInfo getSuccessor ( ServerInfo newNode ) {
		ServerInfo successor;
		int nodeIndex = this.activeServers.indexOf ( newNode );
		try {
			successor = this.activeServers.get ( nodeIndex + 1 );
		} catch ( IndexOutOfBoundsException e ) {
			// that means the new node is the last item and the successor with
			// be the first item.
			successor = this.activeServers.get ( 0 );
		}

		return successor;
	}
	
	
	/**
	 * gets a serverInfo and return two other Server info which
	 * are responsible for keeping the replicated data of the first server
	 * @param s
	 * @return
	 */
	private List<ServerInfo> getReplicas(ServerInfo s){
		if(activeServers.contains(s)){
			int i,r1,r2,l;
			l = activeServers.size();
			i = activeServers.indexOf(s);
			r1 = i + 1;
			r2 = i + 2;
			r1 %= l;
			r2 %= l;
			ArrayList<ServerInfo> replicas = new ArrayList<ServerInfo>();
			// first add the replica which is closer to the new node
			replicas.add(activeServers.get(r1));
			replicas.add(activeServers.get(r2));
			return replicas;
		}
		else return null;
	}
	
	
	/**
	 * returns the two server which, the server s is keeping their replicated data
	 * @param s
	 * @return
	 */
	private List<ServerInfo> getMasters(ServerInfo s){
		if(activeServers.contains(s)){
			int i,m1,m2,l;
			i = activeServers.indexOf(s);
			l = activeServers.size();
			m1 = i -1;
			m2 = i -2;
			if(m1 < 0)
				m1 += l;
			if(m2 < 0)
				m2 += l;
			ArrayList<ServerInfo> masters = new ArrayList<ServerInfo>();
			//first add the master which is far from the node
			masters.add(activeServers.get(m2));
			masters.add(activeServers.get(m1));
			return masters;
		}
		else return null;
	}
	
	

	/**
	 * Gets a new node from repo to start
	 * 
	 * @return returns the node to start or null in case no node was found.
	 */
	private ServerInfo getAvailableNode () {
		for ( ServerInfo server : this.serverRepository ) {
			if ( ! server.isServerLaunched () ) {
				return server;
			}
		}

		return null;
	}

	
	/**
	 * removes a node randomly from the active servers!
	 */
	@Override
	public boolean removeNode () {
		int removeIndex = pickRandomValue ( this.activeServers.size () , true );
		logger.debug ( "Picked node index to remove " + removeIndex );
		ServerInfo nodeToDelete = this.activeServers.get ( removeIndex );
		ServerInfo successor = getSuccessor ( nodeToDelete );
		if ( nodeToDelete.equals ( successor ) ) {
			logger.error ( "Cannot remove node because it is the only active node available, If you want to remove please use the shutdown option" );
			return false;
		}
		else{
			return removeNode(nodeToDelete);
		}
		
	}
	
	/**
	 * remove the nodeToDelete node from the system
	 * @param failed
	 * @return
	 */
	private boolean removeNode(ServerInfo nodeToDelete){
		ServerInfo successor = getSuccessor ( nodeToDelete );
		List <ServerConnection> locked = new ArrayList<ServerConnection>();
		List <ServerInfo> masters = getMasters(nodeToDelete);
		List <ServerInfo> replicas = getReplicas(nodeToDelete);
		ServerConnection nodeToDeleteChannel = this.activeConnections
				.get ( nodeToDelete );
		ServerConnection successorChannel = this.activeConnections
				.get ( successor );
	
		logger.debug("$$$$$$ SYSTEM BEFORE REMOVING $$$$$$");
		for(ServerInfo s: activeServers)
			logger.debug(s.getPort());
		logger.debug("$$$$$$");
				
		this.activeServers.remove ( nodeToDelete );
		calculateMetaData ( this.activeServers );

		// special cases when we would have just one or two nodes left after removal
		if(activeServers.size() <=2 ){	
			logger.debug("<<<< invoking transfers of data >>>>");
			sendMetaData(); // same as to sending metaData to replicas because just two node is left
			int result = sendData(nodeToDelete, successor, nodeToDelete.getFromIndex (),nodeToDelete.getToIndex ());
			
			// telling to the remaining Nodes to replicate their data on each other
			if(activeServers.size() == 2 ){
				result += sendData(activeServers.get(0), activeServers.get(1), activeServers.get(0).getFromIndex(), activeServers.get(0).getToIndex());
				result += sendData(activeServers.get(1), activeServers.get(0), activeServers.get(1).getFromIndex(), activeServers.get(1).getToIndex());
				locked.add(activeConnections.get(activeServers.get(0)));
				locked.add(activeConnections.get(activeServers.get(1)));
				
				logger.debug("Data for replication sent from " + activeServers.get(0).getPort() + " to "
						+ activeServers.get(1).getPort());
					logger.debug("Data sent for replication from " + activeServers.get(1).getPort() + " to "
						+ activeServers.get(0).getPort());	
			}
			if(result == 0){
				// everything went perfect

				logger.debug("Data sent from " + nodeToDelete.getPort() + " to "
						+ successor.getPort());
			}	
			else{
				// move data from nodeToDelet failed
				//getting back the system to the previous stage:(before removing)
				logger.error("SendData Unsuccessful! Delete Node Operation failed" );
				logger.error("Getting back the system to the previous state ");
				try {
					this.activeServers.add(nodeToDelete);
					ECSMessage releaseLock = new ECSMessage ();
					releaseLock.setActionType ( ECSCommand.RELEASE_LOCK );
					sendECSCommand(nodeToDeleteChannel, releaseLock);
					sendECSCommand(new ServerConnection(masters.get(0)), releaseLock);
					sendECSCommand(new ServerConnection(masters.get(1)), releaseLock);
					calculateMetaData(activeServers);
					sendMetaData();
					return false;
				} catch ( IOException e ) {
					logger.error ( "Write lock message couldn't be sent."
						+ e.getMessage () );
					}
			}
		}
		/* in normal cases when we have more than 2 nodes left after removal*/
		else{
			//1.s Send meta-data update to the successor node
			ECSMessage metaDataUpdate = new ECSMessage ();
			metaDataUpdate.setActionType ( ECSCommand.SEND_METADATA );
			metaDataUpdate.setMetaData ( activeServers );
			try {
				sendECSCommand(successorChannel, metaDataUpdate);
			} catch ( IOException e ) {
				logger.error ( "Meta-data update couldn't be sent."
						+ e.getMessage () );
			}
	
			logger.debug("<<<< invoking transfers of data >>>>");
	
			//2. Invoke the transfer of the affected data items
			//3.invoke the two masters to send their data as replicas to the two replicas			
			
			//Sending metaData updates to two new replicas
			try {
				// we dont send metaData update to replica(0) because replica(0) is same as successor!
				ServerConnection replicaChannel = new ServerConnection(replicas.get(1));
				sendECSCommand(replicaChannel, metaDataUpdate);
			} catch ( IOException e ) {
				logger.error ( "Meta-data update couldn't be sent."
						+ e.getMessage () );
			}
	
			if(sendData(nodeToDelete, successor, nodeToDelete.getFromIndex (),nodeToDelete.getToIndex ()) == 0 
				&&
				sendData(masters.get(0), replicas.get(0), masters.get(0).getFromIndex(), masters.get(0).getToIndex()) == 0 
				&&
				sendData(masters.get(1), replicas.get(1), masters.get(1).getFromIndex(), masters.get(1).getToIndex()) == 0)
			{
				locked.add(activeConnections.get(masters.get(0)));
				locked.add(activeConnections.get(masters.get(1)));
					
				logger.debug("Data sent from " + nodeToDelete.getPort() + " to "
						+ successor.getPort());
				logger.debug("Data for replication sent from " + masters.get(0).getPort() + " to "
						+ replicas.get(0).getPort());
				logger.debug("Data sent for replication from " + masters.get(1).getPort() + " to "
						+ replicas.get(1).getPort());
					
				/*4.tell to the next two nodes (newNodes replicas) to delete their replicated data which they 
				* used to store from newNode's Masters
				* we don't send remove data to the replica(0) because replica(0) is the successor and owns the data
				* which used to belong to the node to be removed
				*/
				int r = removeData(replicas.get(1), nodeToDelete.getFromIndex(), nodeToDelete.getToIndex());
				if(r == 0 )
					logger.debug("remove data message was sent to two new replicas : " +
							replicas.get(0).getPort() + "   " + replicas.get(1).getPort());
				else
					logger.warn("remove data message could not be sent to two new replicas : " +
							replicas.get(0).getPort() + "   " + replicas.get(1).getPort());
				
				//5. send metadat update to all
				sendMetaData();
				
			}
			else{
				// move data from nodeToDelet failed
				//getting back the system to the previous stage:(before removing)
				logger.error("SendData Unsuccessful! Delete Node Operation failed" );
				logger.error("Getting back the system to the previous state ");
				try {
					this.activeServers.add(nodeToDelete);
					ECSMessage releaseLock = new ECSMessage ();
					releaseLock.setActionType ( ECSCommand.RELEASE_LOCK );
					sendECSCommand(nodeToDeleteChannel, releaseLock);
					sendECSCommand(new ServerConnection(masters.get(0)), releaseLock);
					sendECSCommand(new ServerConnection(masters.get(1)), releaseLock);
					calculateMetaData(activeServers);
					sendMetaData();
					return false;
				} catch ( IOException e ) {
					logger.error ( "Write lock message couldn't be sent."
							+ e.getMessage () );
					}
			}	
		}	
		
		//6. shut down the nodetoDelete
		ECSMessage shutDown = new ECSMessage ();
		shutDown.setActionType ( ECSCommand.SHUT_DOWN );
		try {
			sendECSCommand(nodeToDeleteChannel, shutDown);
		} catch ( IOException e ) {
			logger.error ( "shut down message couldn't be sent."
				+ e.getMessage () );
			}

		//7. step releasing all locks
		logger.debug("releasing all locks!! ");
		ECSMessage releaseLock = new ECSMessage ();
		releaseLock.setActionType ( ECSCommand.RELEASE_LOCK );
		try {	
			for(ServerConnection sChannel:locked){
				sendECSCommand(sChannel, releaseLock);
				logger.debug("lock on " + sChannel.getServer().getPort() + " released");
			}
		logger.debug ( "All locks are released." );
		} catch ( IOException e ) {
			logger.error ( "ReLease Lock message couldn't be sent." );
		}
		
		//8.
		nodeToDeleteChannel.disconnect();
		cleanUpNode ( nodeToDelete );
		
		logger.debug("$$$$$$ New SYSTEM STATE $$$$$$");
		for(ServerInfo s: activeServers)
			logger.debug(s.getPort());
		logger.debug("$$$$$$");
		
		return true;

	}

	
	/**
	 * tries to recover the System when a failure is detected
	 * @param failed
	 */
	private void recovery(ServerInfo failedNode){
		
		List <ServerConnection> locked = new ArrayList<ServerConnection>();
		// The failed node has not been removed from activeServers yet --> the real activeServers.size() is one value less
		if(activeServers.size() == 1 )
			logger.error("System failed! All nodes are dead! All data is lost!");
		else if(activeServers.size() == 2 ){
			calculateMetaData(activeServers);
			sendMetaData();
			//TODO on server side
			// send a command to store replicated data as the main data on the remaining server
		}else if(activeServers.size() == 3){
			calculateMetaData(activeServers);
			sendMetaData();
			//TODO on the server side
			// send a message to the successor to store replicated data of the failed node as its own data 
			activeServers.remove(failedNode);
			activeConnections.remove(failedNode);
			sendData(activeServers.get(0), activeServers.get(1), activeServers.get(0).getFromIndex(), activeServers.get(0).getToIndex());
			sendData(activeServers.get(1), activeServers.get(0), activeServers.get(1).getFromIndex(), activeServers.get(1).getToIndex());
			locked.add(activeConnections.get(activeServers.get(0)));
			locked.add(activeConnections.get(activeServers.get(1)));
			
			logger.debug("Data for replication sent from " + activeServers.get(0).getPort() + " to "
					+ activeServers.get(1).getPort());
			logger.debug("Data sent for replication from " + activeServers.get(1).getPort() + " to "
					+ activeServers.get(0).getPort());	
		}	
		else{
			ServerInfo successor = getSuccessor ( failedNode );
			List <ServerInfo> masters = getMasters(failedNode);
			List <ServerInfo> replicas = getReplicas(failedNode);
			
			activeServers.remove(failedNode);
			activeConnections.remove(failedNode);
			calculateMetaData(activeServers);
			//1. sending new metaData
			sendMetaData();
			
			//2. recovering lost data from the replicas
			if(sendData(replicas.get(0), successor, failedNode.getFromIndex(), failedNode.getToIndex()) !=0 ){
				if( sendData(replicas.get(1), successor, failedNode.getFromIndex(), failedNode.getToIndex()) == 0)
					logger.info("Data recovered from replica " + replicas.get(1).getPort()
							+ " sent to " + successor.getPort());
				else
					logger.warn(" Data owned by " + failedNode.getPort() + " could not be recovered");
	
			}
			logger.info("Data recovered from replica " + replicas.get(0).getPort()
					+ " sent to " + successor.getPort());
			
			//3. storing data as replica : sent from failed node's masters to failed node's replicas
			if(sendData(masters.get(0), replicas.get(0), masters.get(0).getFromIndex(), masters.get(0).getToIndex()) == 0)
				logger.debug("Data for replication sent from " + masters.get(0).getPort() + " to "
						+ replicas.get(0).getPort());
			else
				logger.warn("Data transfer for replication  from " + masters.get(0).getPort() + " to "
						+ replicas.get(0).getPort() + " FAILED");
					
			if(sendData(masters.get(1), replicas.get(1), masters.get(1).getFromIndex(), masters.get(1).getToIndex()) == 0)
				logger.debug("Data sent for replication from " + masters.get(1).getPort() + " to "
						+ replicas.get(1).getPort());
			else
				logger.warn("Data transfer for replication  from " + masters.get(1).getPort() + " to "
						+ replicas.get(1).getPort() + " FAILED");
				
			locked.add(activeConnections.get(masters.get(0)));
			locked.add(activeConnections.get(masters.get(1)));
		}
			
		//4. releasing locks
		logger.debug("releasing all locks!! ");
		ECSMessage releaseLock = new ECSMessage ();
		releaseLock.setActionType ( ECSCommand.RELEASE_LOCK );
		try {	
			for(ServerConnection sChannel:locked){
				sendECSCommand(sChannel, releaseLock);
				logger.debug("lock on " + sChannel.getServer().getPort() + " released");
			}
		} catch ( IOException e ){
			logger.error ( "ReLease Lock message couldn't be sent." );
		}
		
		cleanUpNode ( failedNode );
		
		//5. adding a new node to the system
		addNode();
		
		logger.debug("$$$$$$ New SYSTEM STATE $$$$$$");
		for(ServerInfo s: activeServers)
			logger.debug(s.getPort());
		logger.debug("$$$$$$");

	}
	
	
	/**
	 * tells the sender to send data in range(fromIndex,toIndex) to the reciever
	 * @param Sender : (ServerInfo)The KVServer that sends the data
	 * @param reciever: (ServerINfo) The KVServer that recieves the data 
	 * @param fromIndex
	 * @param toIndex
	 * @return 0 in case of success and -1 otherwise
	 */
	private int sendData(ServerInfo sender, ServerInfo reciever, String fromIndex, String toIndex){
		
		// Set write lock (lockWrite()) on the sender node
		/*ServerConnection serverChannel = this.activeConnections
				.get ( sender );*/
		// in order to handle concurrent send from the same serverConnection I used new objects!
		ServerConnection senderChannel = new ServerConnection(sender);
		ECSMessage writeLock = new ECSMessage ();
		writeLock.setActionType ( ECSCommand.SET_WRITE_LOCK );
		try {
			senderChannel.connect();
			senderChannel.sendMessage ( writeLock );
		} catch ( IOException e ) {
			logger.error ( "WriteLock message couldn't be sent to " + sender.getPort() );
			senderChannel.disconnect();
			return -1;
		}

		logger.debug ( "Sender node " + sender + " reciever node "
				+ reciever  );
		try {
			Thread.sleep ( 200 );
		} catch ( InterruptedException e1 ) {
		}

		// Invoke the transfer of the affected data items
		ECSMessage moveDataMessage = new ECSMessage ();
		moveDataMessage.setActionType ( ECSCommand.MOVE_DATA );
		moveDataMessage.setMoveFromIndex ( fromIndex );
		moveDataMessage.setMoveToIndex ( toIndex );
		moveDataMessage.setMoveToServer ( reciever );
		try {
			senderChannel.sendMessage ( moveDataMessage );
			Thread temp = new Thread(senderChannel);
			temp.start();
			//3000 is timeout
			synchronized (temp) {
			temp.wait(3000);
			}
			// sender channel got the Ack message
			if(senderChannel.gotResponse()){
				senderChannel.setResponse(false);
				senderChannel.disconnect();
				
				return 0;
			}
			else{
				logger.warn(" TimeOut reached ! could not recieve Message from "
					+ senderChannel.getServer().getPort());
				senderChannel.disconnect();
				senderChannel.setResponse(false);
				return -1;
			}
			
		} catch ( IOException e ) {
			logger.error ( "MoveData message couldn't be sent to  " + sender.getPort() );
			senderChannel.disconnect();
			return -1;
		} catch (InterruptedException e) {
			logger.error ( "MoveData message couldn't be sent to  " + sender.getPort() );
			senderChannel.disconnect();
			return -1;
		}
	}
	

	/**
	 * tell the Server s to remove one of its replication storages
	 */
	private int removeData(ServerInfo server, String fromIndex, String toIndex){
		// Invoke the removal of the replicated data items
		ECSMessage removeDataMessage = new ECSMessage ();
		removeDataMessage.setActionType ( ECSCommand.REMOVE_DATA );
		removeDataMessage.setMoveFromIndex ( fromIndex );
		removeDataMessage.setMoveToIndex ( toIndex );
		
		try {
			/*ServerConnection serverChannel = this.activeConnections
					.get ( server );*/
			// in order to handle concurrent send from the same serverConnection I used new objects!
			
			ServerConnection serverChannel = new ServerConnection(server);
			serverChannel.connect();
			serverChannel.sendMessage ( removeDataMessage );
			Thread temp = new Thread(serverChannel);
			temp.start();	
			//3000 is timeout
			synchronized (temp) {
				temp.wait(3000);
			}
			// sender channel got the Ack message
			if(serverChannel.gotResponse()){
				serverChannel.setResponse(false);
				serverChannel.disconnect();
				
				return 0;
			}
			else{
				logger.warn(" TimeOut reached ! could not recieve Message from "
					+ serverChannel.getServer().getPort());
				serverChannel.disconnect();
				serverChannel.setResponse(false);
				return -1;
			}	
		}catch ( IOException e ) {
			logger.error ( "RemoveData message couldn't be sent to  " + server.getAddress() + ":" + server.getPort() );
			return -1;
		}catch(InterruptedException e){
			logger.error ( "RemoveData message couldn't be sent to  " + server.getAddress() + ":" + server.getPort() );
			return -1;
		}
	}
	
	/**
	 * send metaData to the activeServers (in case of initialization or metaData Update) 
	 * @return
	 */
	private int sendMetaData(){
		// send meta data 
		ECSMessage metaDataUpdate = new ECSMessage ();
		metaDataUpdate.setActionType ( ECSCommand.SEND_METADATA );
		metaDataUpdate.setMetaData ( activeServers );
		for ( ServerInfo server : this.activeServers ) {
			try {
				ServerConnection serverChannel = activeConnections
						.get ( server );
				serverChannel.connect();
				serverChannel.sendMessage ( metaDataUpdate );
				serverChannel.disconnect();
			} catch ( IOException e ) {
				logger.error ( "Could not send message to server"
						+ server + e.getMessage () );
				return -1;
				}
			}
		
			logger.debug ( "Updated Meta-data handed to servers." );	
			return 0;
	}
	
	
	private void cleanUpNode ( ServerInfo nodeToDelete ) {
		for ( ServerInfo server : this.serverRepository ) {
			if ( server.equals ( nodeToDelete ) ) {
				server.setServerLaunched ( false );
				this.activeConnections.remove ( nodeToDelete );
				this.activeServers.remove ( nodeToDelete );
				break;
			}
		}

	}

	public List<ServerInfo> getServerRepository() {
	    return serverRepository;
	}

	public List<ServerInfo> getActiveServers() {
	    return activeServers;
	}

	public Map<ServerInfo, ServerConnection> getActiveConnections() {
	    return activeConnections;
	}
	
	private void sendECSCommand(ServerConnection channel, ECSMessage message) throws IOException{
		channel.connect();
		channel.sendMessage ( message );
		channel.disconnect();
	}

	public void reportFailure(ServerInfo failedServer, ServerInfo reportee) {
		logger.debug("Failure detected Failed:" + failedServer + " reporter:"
				+ reportee);

		for (ServerInfo serverInfo : activeServers) {
			if (serverInfo.equals(failedServer)) {
				serverInfo.reportFailure(reportee);
			}

			if (serverInfo.getNumberofFailReports() > 1) {
				// /TODO: @Arash add your recovery codes
				logger.debug("Failure detected more than one server reported");
			}
		}

	}

}