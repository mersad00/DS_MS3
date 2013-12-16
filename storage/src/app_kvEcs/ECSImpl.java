package app_kvEcs;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import org.apache.log4j.Logger;




import utilities.LoggingManager;
import common.Hasher;
import common.ServerInfo;

public class ECSImpl implements ECS {

    private List<ServerInfo> serverRepository;
    /**
     * The meta data
     */
    private List<ServerInfo> activeServers;
    Logger logger = LoggingManager.getInstance().createLogger(this.getClass());
    private Hasher md5Hasher ;

    /**
     * @param fileName
     *            : the name of the configuration file
     * @throws FileNotFoundException
     */
    public ECSImpl(String fileName) throws FileNotFoundException {
	this.md5Hasher = new Hasher();
	// parse the server repository
	readServerInfo(fileName);
	initService(pickRandomValue(serverRepository.size()));
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

    /** Generates a random number in range
     * @param size: the range upper bound
     * @return
     */
    private int pickRandomValue(int size) {
	Random randomGenerator = new Random();
	int randomNumber = randomGenerator.nextInt(size);

	logger.info("Picked "+randomNumber+" as a random number.");
	return randomNumber;
    }
    
    

    @Override
    public void initService(int numberOfNodes) {
	List<ServerInfo> serversToStart = this.serverRepository.subList(0, numberOfNodes);
	
	logger.info("ECS will launch "+numberOfNodes+" servers");
	logger.debug("ECS will launch"+ serversToStart);
	// calculate the metaData
	serversToStart = calculateMetaData(serversToStart);
	launchNodes(serversToStart);
	
	
	//communicate with servers and send call init
	ECSMessage initMessage = getInitMessage(serversToStart);
	// TODO: call the communicator with server to send this message
	logger.info("Sending init signals to servers");
	
	//TODO collect the acks from server
	
	
	
	logger.info("Active servers are launched and handed meta-data.");

    }

    private ECSMessage getInitMessage(List<ServerInfo> serversToStart) {
	ECSMessage initMessage = new ECSMessage();
	initMessage.setActionType(ECSCommand.INIT);
	initMessage.setMetaData(serversToStart);
	return initMessage;
    }

    private void launchNodes(List<ServerInfo> serversToStart) {
	for (ServerInfo server: serversToStart){
	    //TODO call ssh
	    server.setServerLaunched(true);
	}
	
    }

    private List<ServerInfo> calculateMetaData(List<ServerInfo> serversToStart) {
	
	for (ServerInfo server: serversToStart){// loop and the hash values
	    String hashKey = md5Hasher.getHash(server.getAddress()+":"+server.getPort());
	    //the to index is the value of the server hash
	    server.setToIndex(hashKey);
	}
	
	Collections.sort(serversToStart, new Comparator<ServerInfo>() { // sort the list by the hashes
	    @Override
	    public int compare(ServerInfo o1, ServerInfo o2) {
	         return md5Hasher.compareHashes(o1.getToIndex() , o2.getToIndex());
	         }
	});
	
	logger.debug("Sorted list of servers "+serversToStart);
	    
	for (int i = 0; i < serversToStart.size(); i++) {
	    ServerInfo server = serversToStart.get(i);
	    ServerInfo predecessor;
	    if(i==0){// first node is a special case.
		predecessor = serversToStart.get(serversToStart.size()-1);
	    }else{
		predecessor = serversToStart.get(i-1);
	    }
	    server.setFromIndex(predecessor.getToIndex());
	    
	}
	this.activeServers = serversToStart;
	logger.debug("Calculated metadata "+serversToStart);
	return serversToStart;
	}
    
    

    public Hasher getMd5Hasher() {
        return md5Hasher;
    }

    @Override
    public void start() {
	//communicate with servers and send call init
	ECSMessage startMessage = new ECSMessage();
	startMessage.setActionType(ECSCommand.START);
	
	// TODO: call the communicator with server to send this message

	//TODO collect the acks from server?

	logger.info("Active servers are started.");
    }

    @Override
    public void shutdown() {
	//communicate with servers and send call init
	ECSMessage shutDownMessage = new ECSMessage();
	shutDownMessage.setActionType(ECSCommand.SHUT_DOWN);

	// TODO: call the communicator with server to send this message

	//TODO collect the acks from server?

	logger.info("Active servers are shutdown.");

    }
    @Override
    public void stop() {
	//communicate with servers and send call init
	ECSMessage stopMessage = new ECSMessage();
	stopMessage.setActionType(ECSCommand.STOP);

	// TODO: call the communicator with server to send this message

	//TODO collect the acks from server?

	logger.info("Active servers are stopped.");

    }

    @Override
    public void addNode() throws IllegalArgumentException {

	// steps to add new node:
	List<ServerInfo> serverList= new ArrayList<ServerInfo>();
	ServerInfo newNode = getAvailableNode();
	if (newNode==null){
	    logger.info("No available node to add."); 
	    throw new IllegalArgumentException("No available node to add");
	}
	
	logger.debug("Adding a new node."+ newNode); 
	serverList.add(newNode);
	// will start the server and set the flag ti true
	launchNodes(serverList);
	logger.debug("New node launched"); 
	this.activeServers.add(newNode);
	// need to re-calculate the new metadata
	calculateMetaData(activeServers);
	System.out.println("The new node is"+newNode);
	
	//1.init the newNode and start it.
	ECSMessage initMessage = getInitMessage(this.activeServers);
	//TODO: send init to the newNode.
	
	//TODO after ack of the new node send start message newNode
	
	
	logger.debug("New node initiated"); 

	ECSMessage startMessage = new ECSMessage();
	startMessage.setActionType(ECSCommand.START);
	logger.debug("New node started"); 
	
	
	//Set write lock (lockWrite()) on the successor node
	ServerInfo successor = getSuccessor(newNode);
	//TODO send write lock message to successor
	ECSMessage writeLock = new ECSMessage();
	writeLock.setActionType(ECSCommand.SET_WRITE_LOCK);
	
	logger.debug("Successor node"+successor+ " locked."); 
	
	//Invoke the transfer of the affected data items
	ECSMessage moveDataMessage = new ECSMessage();
	moveDataMessage.setActionType(ECSCommand.MOVE_DATA);
	moveDataMessage.setMoveFromIndex(newNode.getFromIndex());
	moveDataMessage.setMoveToIndex(newNode.getToIndex());
	moveDataMessage.setMoveToServer(newNode);
	//TODO send move data to successor
	
	
	//TODO  waite for ack
	logger.debug("Data was moved"); 
	
	// send metadat update
	ECSMessage metaDataUpdate = new ECSMessage();
	metaDataUpdate.setActionType(ECSCommand.SEND_METADATA);
	metaDataUpdate.setMetaData(activeServers);
	//TODO send metadata to activeServers

	
	logger.debug("Updated Meta-data handed to servers."); 
	// release the lock 
	//TODO send write lock message to successor
	ECSMessage releaseLock = new ECSMessage();
	releaseLock.setActionType(ECSCommand.RELEASE_LOCK);

	logger.debug("Successor lock released."); 

    }
    
    private ServerInfo getSuccessor(ServerInfo newNode) {
	ServerInfo successor;
	// TODO double check this works
	int nodeIndex = this.activeServers.indexOf(newNode);
	try {
	    successor = this.activeServers.get(nodeIndex+1);
	} catch (ArrayIndexOutOfBoundsException e) {
	    // that means the new node is the last item and the successor with be the first item.
	    successor = this.activeServers.get(0);
	}
	
	return successor;
    }

    /** Gets a new node from repo to start
     * @return returns the node to start or null in case no node was found.
     */
    private ServerInfo getAvailableNode() {
	for (ServerInfo server : this.serverRepository) {
	    if (!server.isServerLaunched()){
		return server;
	    }
	}
	
	return null;
    }

    @Override
    public boolean removeNode() {
	int removeIndex = pickRandomValue(this.activeServers.size());
	logger.debug("Picked node index to remove "+removeIndex); 
	ServerInfo nodeToDelete = this.activeServers.get(removeIndex);
	ServerInfo successor = getSuccessor(nodeToDelete);
	this.activeServers.remove(removeIndex);
	calculateMetaData(this.activeServers);
	logger.debug("Calculated meta-data without the node to delete");
	
	//TODO send writeLock message to nodeToDelete
	ECSMessage writeLock = new ECSMessage();
	writeLock.setActionType(ECSCommand.SET_WRITE_LOCK);

	logger.debug("Node to delete "+nodeToDelete+ " locked."); 

	//Send meta-data update to the successor node
	ECSMessage metaDataUpdate = new ECSMessage();
	metaDataUpdate.setActionType(ECSCommand.SEND_METADATA);
	metaDataUpdate.setMetaData(activeServers);
	
	//TODO send metaDataUpdate to successor
	
	//Invoke the transfer of the affected data items
	ECSMessage moveDataMessage = new ECSMessage();
	moveDataMessage.setActionType(ECSCommand.MOVE_DATA);
	moveDataMessage.setMoveFromIndex(nodeToDelete.getFromIndex());
	moveDataMessage.setMoveToIndex(nodeToDelete.getToIndex());
	moveDataMessage.setMoveToServer(successor);
	//TODO send moveDataMessage to nodeToDelete
	
	
	logger.debug("Data moved."); 

	// send metadat update
	ECSMessage metaDataUpdateToAll = new ECSMessage();
	metaDataUpdateToAll.setActionType(ECSCommand.SEND_METADATA);
	metaDataUpdateToAll.setMetaData(activeServers);
	//TODO send metadata to activeServers


	//TODO send shutDown message to nodeToDelete
	ECSMessage shutDown = new ECSMessage();
	writeLock.setActionType(ECSCommand.SHUT_DOWN);

	this.serverRepository.get(serverRepository.indexOf(nodeToDelete)).setServerLaunched(false);
	return true;
    }
    
   

}
