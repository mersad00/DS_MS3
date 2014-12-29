package app_kvEcs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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

import utilities.LoggingManager;
import common.Hasher;
import common.ServerInfo;

public class ECSImpl implements ECS {

	private List < ServerInfo > serverRepository;
	/**
	 * The meta data
	 */
	private List < ServerInfo > activeServers;
	private Map < ServerInfo , ServerConnection > activeConnections;
	Logger logger = LoggingManager.getInstance ().createLogger (
			this.getClass () );
	private Hasher md5Hasher;
	//private ProcessInvoker processInvoker;
	private SshInvoker processInvoker;
	private String fileName;
	private boolean localCall;

	/**
	 * @param fileName
	 *            : the name of the configuration file
	 * @throws FileNotFoundException
	 */
	public ECSImpl ( String fileName ) throws FileNotFoundException {
		this.fileName = fileName;
		/* parse the server repository */
		readServerInfo ( this.fileName );
		init ( pickRandomValue ( serverRepository.size () , false ) );		
	}
	
	ECSImpl(int numberOfNodes , String fileName) throws FileNotFoundException{
		this.fileName = fileName;
		/* parse the server repository */
		readServerInfo ( this.fileName );		
		init ( numberOfNodes );	
	}
	
	ECSImpl(int numberOfNodes , String fileName, boolean local) throws FileNotFoundException{
		this.fileName = fileName;
		localCall = local;
		/* parse the server repository */
		readServerInfo ( this.fileName );		
		init ( numberOfNodes );	
	}
	
	public void init (int numberOfNodes ){
		this.md5Hasher = new Hasher ();
		/*this.processInvoker = new ProcessInvoker ();*/
		this.processInvoker = new SshCaller();
		initService ( numberOfNodes );	
	}
	

	private void readServerInfo ( String fileName )
			throws FileNotFoundException {
		Scanner fileReader = new Scanner ( new File ( fileName ) );
		fileReader.useDelimiter ( "\n" );
		serverRepository = new ArrayList < ServerInfo > ();

		while ( fileReader.hasNext () ) {
			serverRepository
					.add ( new ServerInfo ( fileReader.next ().trim () ) );
		}
		fileReader.close ();

	}

	/**
	 * Generates a random number in range
	 * 
	 * @param allowZero
	 * @param size
	 *            : the range upper bound
	 * @return
	 */
	private int pickRandomValue ( int size , boolean allowZero ) {
		Random randomGenerator = new Random ();
		int randomNumber = randomGenerator.nextInt ( size );
		if ( ! allowZero ) {
			randomNumber += 1;
		}

		logger.info ( "Picked " + randomNumber + " as a random number." );
		return randomNumber;
	}

	@Override
	public void initService ( int numberOfNodes ) {
		Random rand = new Random();
		int count = 0;
		ServerInfo temp;
		List < ServerInfo > serversToStart = new ArrayList<ServerInfo>();
		this.activeConnections = new HashMap < ServerInfo , ServerConnection > ();
		if(this.activeServers == null)
			this.activeServers = new ArrayList<ServerInfo>();
		//choosing servers randomly
		while(count < numberOfNodes ){
			int i = rand.nextInt(serverRepository.size()); 
			temp = serverRepository.get(i);
			if((!serversToStart.contains(temp) ) && !this.activeServers.contains(temp)){	
				serversToStart.add(temp);
				count++;
			}
		}
		logger.info ( "ECS will launch " + numberOfNodes + " servers " );
		launchNodes ( serversToStart );
		
		//some nodes were not started successfully! 
		if(serversToStart.size() < numberOfNodes){
			int n = numberOfNodes - serversToStart.size();
			count = 0; 
			int i = 0,r;
			while(count < n && i < (serverRepository.size() -1 )){
				temp = serverRepository.get(i);
				if((!serversToStart.contains(temp) ) && !this.activeServers.contains(temp)){	
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
		
		logger.info("ECS started " +  serversToStart.toString() );
		
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
			} catch ( IOException e ) {
				logger.error ( "One server node couldn't be initiated" + server );
				// this.activeServers.remove(server);
			}

		}

		logger.info ( "Active servers are launched and handed meta-data." );

	}
			
		
		/*List < ServerInfo > serversToStart = this.serverRepository.subList ( 0 ,
				numberOfNodes );
		this.activeConnections = new HashMap < ServerInfo , ServerConnection > ();
		logger.info ( "ECS will launch " + numberOfNodes + " servers" );
		logger.debug ( "ECS will launch" + serversToStart );
		// calculate the metaData
		serversToStart = calculateMetaData ( serversToStart );
		launchNodes ( serversToStart );

		logger.debug ( "Nodes launched by SSH calls." );

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
			} catch ( IOException e ) {
				logger.error ( "One server node couldn't be initiated" + server );
				// this.activeServers.remove(server);
			}

		}

		logger.info ( "Active servers are launched and handed meta-data." );

	}
	*/
	
	
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
				channel.sendMessage ( startMessage );
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
				channel.sendMessage ( shutDownMessage );

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
				channel.sendMessage ( stopMessage );
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
			activeConnections.remove(newNode);
			calculateMetaData(activeServers);
			return;
		}
		
		/*3. tell the sucessor to send data to the newNode
		 * tell the two new Masters to send their data for replication to the newNode
		 */
		ServerInfo successor = getSuccessor ( newNode );
		List <ServerInfo> masters = getMasters(newNode); 
		if(sendData(successor, newNode, newNode.getFromIndex(), newNode.getToIndex()) == 0 &&
				sendData(masters.get(0), newNode, masters.get(0).getFromIndex(), masters.get(0).getToIndex()) == 0 &&
				sendData(masters.get(1), newNode, masters.get(1).getFromIndex(), masters.get(1).getToIndex()) == 0){	
			
			/*4.tell to the next two nodes (newNodes replicas) to delete their replicated data which they 
			 * used to store from newNode's Masters
			 */
			List <ServerInfo> replicas = getReplicas(newNode); 
			removeData(replicas.get(0), masters.get(0).getFromIndex(), masters.get(0).getToIndex());
			removeData(replicas.get(1), masters.get(1).getFromIndex(), masters.get(1).getToIndex());
			
			//5. tell the two replicas to store replicated data from newNode
			if(	sendData(replicas.get(0), newNode, newNode.getFromIndex(), newNode.getToIndex()) == 1 ||
				sendData(replicas.get(1), newNode, newNode.getFromIndex(), newNode.getToIndex()) == 1)
				logger.warn("One or two of the replication operation for node " + newNode.getAddress()
						+ ":" + newNode.getPort() + "failed");
			
			//sends MetaData to all the servers!
			sendMetaData();
			
			//6. release the lock from the successor
			ECSMessage releaseLock = new ECSMessage ();
			releaseLock.setActionType ( ECSCommand.RELEASE_LOCK );
			try {	
			ServerConnection successorChannel = this.activeConnections
					.get ( successor );
			successorChannel.sendMessage ( releaseLock );
			} catch ( IOException e ) {
				logger.error ( "ReLease Lock message couldn't be sent." );
				//TODO
				// what to do in this case!??? should we remove this server 
				//from the list?? or just wait for failure detection system
				//to detect the failure
			}
			
			logger.debug ( "Successor lock released." );
		}
		
		// when move data from successor to the receiver was not successful
		else{
			// data could not be moved to the newly added Server
			logger.error("Could not move data from " + successor.getServerName() +" to " + newNode.getServerName());
			logger.error("Operation addNode Not Successfull");
			activeServers.remove(newNode);
			activeConnections.remove(newNode);
			calculateMetaData(activeServers);
			return;
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
		this.activeServers.remove ( nodeToDelete );
		calculateMetaData ( this.activeServers );
		
		ServerConnection nodeToDeleteChannel = this.activeConnections
				.get ( nodeToDelete );
		ServerConnection successorChannel = this.activeConnections
				.get ( successor );
		
		//1. set lock on the nodeToDelete
		ECSMessage writeLock = new ECSMessage ();
		writeLock.setActionType ( ECSCommand.SET_WRITE_LOCK );
		try {
			nodeToDeleteChannel.sendMessage ( writeLock );
		} catch ( IOException e ) {
			logger.error ( "Write lock message couldn't be sent."
					+ e.getMessage () );
		}

		logger.debug ( "Node to delete " + nodeToDelete + " locked." );

		//2.s Send meta-data update to the successor node
		ECSMessage metaDataUpdate = new ECSMessage ();
		metaDataUpdate.setActionType ( ECSCommand.SEND_METADATA );
		metaDataUpdate.setMetaData ( activeServers );
		try {
			successorChannel.sendMessage ( metaDataUpdate );
		} catch ( IOException e ) {
			logger.error ( "Meta-data update couldn't be sent."
					+ e.getMessage () );
		}

		try {
			Thread.sleep ( 200 );
		} catch ( InterruptedException e1 ) {
		}
		
		//3. Invoke the transfer of the affected data items
		if(sendData(nodeToDelete, successor, nodeToDelete.getFromIndex (),
				nodeToDelete.getToIndex ()) == 0){
				
			//4. send metadat update to all
			sendMetaData();
			
			//5. shut down the nodetoDelete
			ECSMessage shutDown = new ECSMessage ();
			shutDown.setActionType ( ECSCommand.SHUT_DOWN );
			try {
				nodeToDeleteChannel.sendMessage ( shutDown );
			} catch ( IOException e ) {
				logger.error ( "shut down message couldn't be sent."
					+ e.getMessage () );
				}
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
				nodeToDeleteChannel.sendMessage ( releaseLock );
				calculateMetaData(activeServers);
				sendMetaData();
				return false;
			} catch ( IOException e ) {
				logger.error ( "Write lock message couldn't be sent."
						+ e.getMessage () );
			}
		}
		
		
		cleanUpNode ( nodeToDelete );
		return true;

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
		ServerConnection senderChannel = this.activeConnections
				.get ( sender );
		ECSMessage writeLock = new ECSMessage ();
		writeLock.setActionType ( ECSCommand.SET_WRITE_LOCK );
		try {
			senderChannel.sendMessage ( writeLock );
		} catch ( IOException e ) {
			logger.error ( "WriteLock message couldn't be sent to " + sender.getPort() );
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
		if ( senderChannel.receiveMessage ().getActionType () == ECSCommand.ACK )
			return 0;
		} catch ( IOException e ) {
			logger.error ( "MoveData message couldn't be sent to  " + sender.getPort() );
			return -1;
		}
		return -1;
	}
	

	/**
	 * tell the Server s to remove one of its replication storages
	 */
	private int removeData(ServerInfo server, String fromIndex, String toIndex){
		// Invoke the removal of the replicated data items
		//TODO change the ECSMessage if a new ECSMessage is defined for data removal 
		ECSMessage moveDataMessage = new ECSMessage ();
		moveDataMessage.setActionType ( ECSCommand.REMOVE_DATA );
		moveDataMessage.setMoveFromIndex ( fromIndex );
		moveDataMessage.setMoveToIndex ( toIndex );
		moveDataMessage.setMoveToServer ( null );
		try {
			ServerConnection serverChannel = this.activeConnections
					.get ( server );
			serverChannel.connect();
			serverChannel.sendMessage ( moveDataMessage );	
			if ( serverChannel.receiveMessage ().getActionType () == ECSCommand.ACK ){
				logger.info("Delete data Message sent to " + server.getAddress() + ":" + server.getPort());
				return 0;
			}
		}catch ( IOException e ) {
			logger.error ( "MoveData message couldn't be sent to  " + server.getAddress() + ":" + server.getPort() );
			return -1;
		}
		return -1;

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
				serverChannel.sendMessage ( metaDataUpdate );
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

	
	
}