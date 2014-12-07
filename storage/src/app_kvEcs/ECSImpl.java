package app_kvEcs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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
		List < ServerInfo > serversToStart = this.serverRepository.subList ( 0 ,
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
		for(ServerInfo server: serversToStart){
			arguments[0] = String.valueOf(server.getPort());
			processInvoker.invokeProcess(server.getAddress(), command, arguments);
		}
		
		

	}

	private List < ServerInfo > calculateMetaData (
			List < ServerInfo > serversToStart ) {

		for ( ServerInfo server : serversToStart ) {// loop and the hash values
			String hashKey = md5Hasher.getHash ( server.getAddress () + ":"
					+ server.getPort () );
			// the to index is the value of the server hash
			server.setToIndex ( hashKey );
		}

		Collections.sort ( serversToStart , new Comparator < ServerInfo > () { // sort
																				// the
																				// list
																				// by
																				// the
																				// hashes
					@Override
					public int compare ( ServerInfo o1 , ServerInfo o2 ) {
						return md5Hasher.compareHashes ( o1.getToIndex () ,
								o2.getToIndex () );
					}
				} );

		logger.debug ( "Sorted list of servers " + serversToStart );

		for ( int i = 0 ; i < serversToStart.size () ; i++ ) {
			ServerInfo server = serversToStart.get ( i );
			ServerInfo predecessor;
			if ( i == 0 ) {// first node is a special case.
				predecessor = serversToStart.get ( serversToStart.size () - 1 );
			} else {
				predecessor = serversToStart.get ( i - 1 );
			}
			server.setFromIndex ( predecessor.getToIndex () );

		}
		this.activeServers = serversToStart;
		logger.debug ( "Calculated metadata " + serversToStart );
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

	@Override
	public void addNode () {

		// steps to add new node:
		List < ServerInfo > serverList = new ArrayList < ServerInfo > ();
		ServerInfo newNode = getAvailableNode ();
		if ( newNode == null ) {
			logger.info ( "No available node to add." );
			return;
		}

		logger.debug ( "Adding a new node." + newNode );
		serverList.add ( newNode );
		// will start the server and set the flag ti true
		launchNodes ( serverList );
		logger.debug ( "New node launched" );
		this.activeServers.add ( newNode );
		// need to re-calculate the new metadata
		calculateMetaData ( activeServers );
		System.out.println ( "The new node is" + newNode );

		// 1.init the newNode and start it.
		ECSMessage initMessage = getInitMessage ( this.activeServers );
		ServerConnection channel = new ServerConnection ( newNode );
		try {
			channel.connect ();
			channel.sendMessage ( initMessage );
			activeConnections.put ( newNode , channel );
		} catch ( IOException e ) {
			logger.error ( "One server node couldn't be initiated" + newNode );
		}

		logger.debug ( "New node initiated" );

		ECSMessage startMessage = new ECSMessage ();
		startMessage.setActionType ( ECSCommand.START );
		try {
			channel.sendMessage ( startMessage );
		} catch ( IOException e ) {
			logger.error ( "Start message couldn't be sent." );
		}
		logger.debug ( "New node started" );

		// Set write lock (lockWrite()) on the successor node
		ServerInfo successor = getSuccessor ( newNode );
		ServerConnection successorChannel = this.activeConnections
				.get ( successor );
		ECSMessage writeLock = new ECSMessage ();
		writeLock.setActionType ( ECSCommand.SET_WRITE_LOCK );
		try {
			successorChannel.sendMessage ( writeLock );
		} catch ( IOException e ) {
			logger.error ( "Start message couldn't be sent." );
		}

		logger.debug ( "Successor node" + successor + " locked."
				+ successorChannel.getServer () );
		try {
			Thread.sleep ( 200 );
		} catch ( InterruptedException e1 ) {
		}

		// Invoke the transfer of the affected data items
		ECSMessage moveDataMessage = new ECSMessage ();
		moveDataMessage.setActionType ( ECSCommand.MOVE_DATA );
		moveDataMessage.setMoveFromIndex ( newNode.getFromIndex () );
		moveDataMessage.setMoveToIndex ( newNode.getToIndex () );
		moveDataMessage.setMoveToServer ( newNode );
		try {
			successorChannel.sendMessage ( moveDataMessage );
		} catch ( IOException e ) {
			logger.error ( "Start message couldn't be sent." );
		}

		try {
			if ( successorChannel.receiveMessage ().getActionType () == ECSCommand.ACK ) {

				// send metadat update
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
					}
				}
				logger.debug ( "Updated Meta-data handed to servers." );
				// release the lock
				ECSMessage releaseLock = new ECSMessage ();
				releaseLock.setActionType ( ECSCommand.RELEASE_LOCK );
				try {
					successorChannel.sendMessage ( releaseLock );
				} catch ( IOException e ) {
					logger.error ( "Start message couldn't be sent." );
				}

				logger.debug ( "Successor lock released." );
			}
		} catch ( IOException e ) {
			logger.error ( e.getMessage () );
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

	@Override
	public boolean removeNode () {
		int removeIndex = pickRandomValue ( this.activeServers.size () , true );
		logger.debug ( "Picked node index to remove " + removeIndex );
		ServerInfo nodeToDelete = this.activeServers.get ( removeIndex );
		ServerInfo successor = getSuccessor ( nodeToDelete );
		this.activeServers.remove ( removeIndex );
		if ( nodeToDelete.equals ( successor ) ) {
			logger.error ( "Cannot remove node because it is the only active node available, If you want to remove please use the shutdown option" );
			return false;
		}
		calculateMetaData ( this.activeServers );
		logger.debug ( "Calculated meta-data without the node to delete" );
		ServerConnection nodeToDeleteChannel = this.activeConnections
				.get ( nodeToDelete );
		ServerConnection successorChannel = this.activeConnections
				.get ( successor );
		ECSMessage writeLock = new ECSMessage ();
		writeLock.setActionType ( ECSCommand.SET_WRITE_LOCK );
		try {
			nodeToDeleteChannel.sendMessage ( writeLock );
		} catch ( IOException e ) {
			logger.error ( "Write lock message couldn't be sent."
					+ e.getMessage () );
		}

		logger.debug ( "Node to delete " + nodeToDelete + " locked." );

		// Send meta-data update to the successor node
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
		// Invoke the transfer of the affected data items
		ECSMessage moveDataMessage = new ECSMessage ();
		moveDataMessage.setActionType ( ECSCommand.MOVE_DATA );
		moveDataMessage.setMoveFromIndex ( nodeToDelete.getFromIndex () );
		moveDataMessage.setMoveToIndex ( nodeToDelete.getToIndex () );
		moveDataMessage.setMoveToServer ( successor );

		try {
			nodeToDeleteChannel.sendMessage ( moveDataMessage );
		} catch ( IOException e ) {
			logger.error ( "move data message couldn't be sent."
					+ e.getMessage () );
		}

		logger.debug ( "Data moved." );
		try {
			if ( nodeToDeleteChannel.receiveMessage ().getActionType () == ECSCommand.ACK ) {

				// send metadat update
				ECSMessage metaDataUpdateToAll = new ECSMessage ();
				metaDataUpdateToAll.setActionType ( ECSCommand.SEND_METADATA );
				metaDataUpdateToAll.setMetaData ( activeServers );
				for ( ServerInfo server : this.activeServers ) {
					try {
						ServerConnection serverChannel = activeConnections
								.get ( server );
						serverChannel.sendMessage ( metaDataUpdateToAll );
					} catch ( IOException e ) {
						logger.error ( "Could not send message to server"
								+ server + e.getMessage () );
					}

					ECSMessage shutDown = new ECSMessage ();
					shutDown.setActionType ( ECSCommand.SHUT_DOWN );

					try {
						nodeToDeleteChannel.sendMessage ( shutDown );
					} catch ( IOException e ) {
						logger.error ( "shut down message couldn't be sent."
								+ e.getMessage () );
					}

				}
			}
		} catch ( IOException e ) {
			e.printStackTrace ();
		}
		cleanUpNode ( nodeToDelete );

		return true;
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