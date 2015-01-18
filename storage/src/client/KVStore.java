/**
 * This class is the client library for the communication 
 * with the servers, containing all the required methods
 * like connect , put , get.
 * 
 * <p> On each operation, the metadata is checked to determine
 * the correct server that contains the specific key, and send
 * the request to this server.
 * 
 * @see KVMEssage
 * @see Socket
 * @see ServerInfo;
 * 
 */
package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import common.Cache;
import common.CacheStrategy;
import common.Hasher;
import common.ServerInfo;
import common.messages.AbstractMessage;
import common.messages.AbstractMessage.MessageType;
import common.messages.KVMessage;
import common.messages.ClientMessage;
import common.messages.KVMessage.StatusType;
import common.messages.SubscribeMessage;
import common.messages.UnsubscribeMessage;

public class KVStore implements KVCommInterface {

	private Logger logger = Logger.getRootLogger();
	private ServerInfo currentDestinationServer;
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	private List<ServerInfo> metadata;
	private AbstractMessage lastSentMessage;
	private SubscribtionCache dataStoreCache;
	private static ClientInfo paretnInfo;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address
	 *            the address of the KVServer
	 * @param port
	 *            the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this(new ServerInfo(address, port));
		//dataStoreCache = new Cache(1000, CacheStrategy.LRU);
		dataStoreCache = new SubscribtionCache(1000, CacheStrategy.LRU);
	}

	public KVStore(ServerInfo serverInfo) {
		this.currentDestinationServer = serverInfo;
		this.metadata = new ArrayList<ServerInfo>();
	}
	
	public void setparentInfo(ClientInfo parent){
		paretnInfo = parent;
		
	}

	/**
	 * connect to the specified address and port, and get the output and input
	 * streams from the connection to be used in communication
	 */
	@Override
	public void connect() throws IOException {
		try {
			clientSocket = new Socket(currentDestinationServer.getAddress(),
					currentDestinationServer.getPort());
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			logger.info("Connection established with "
					+ currentDestinationServer.toString());
		} catch (IOException ioe) {
			logger.error("Connection could not be established!");
			throw ioe;
		}
	}

	/**
	 * switch connection to another server in case of the current connected
	 * server is not the correct server.
	 * 
	 * @param newDestinationServerInfo
	 *            the new server details to switch connection to
	 * @throws IOException
	 */
	public void switchConnection(ServerInfo newDestinationServerInfo)
			throws IOException {
		this.tearDownConnection();
		this.currentDestinationServer = newDestinationServerInfo;
		logger.info("switch connection to "
				+ currentDestinationServer.toString());
		this.connect();
	}

	/**
	 * this method is used to disconnect the current connected server
	 */
	@Override
	public void disconnect() {
		try {
			tearDownConnection();
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	private void tearDownConnection() throws IOException {
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			input.close();
			output.close();
			clientSocket.close();
			clientSocket = null;

			logger.info("connection closed!");
		}
	}

	/**
	 * sends request to the correct server to insert the key and the value,
	 * update or delete it.
	 * 
	 * @param key
	 *            the key to be inserted
	 * @param value
	 *            the value to be inserted
	 * 
	 * @return KVMessage represents the result of this operation
	 */
	@Override
	public KVMessage put(String key, String value) throws IOException {
		ClientMessage msg = new ClientMessage();
		KVMessage receivedMsg = null;
		msg.setKey(key);
		msg.setValue(value);
		msg.setStatus(KVMessage.StatusType.PUT);
		// ServerInfo tempInfo = this.getDestinationServerInfo ( key );
		try {
			/*
			 * if ( ! this.currentDestinationServer.equals ( tempInfo ) ) {
			 * this.switchConnection ( tempInfo ); }
			 */
			this.sendMessage(msg);
			receivedMsg = this.receiveMessage();

		} catch (IOException e) {
			logger.error("error in sending or receiving message");
			throw e;
		}
		return receivedMsg;
	}

	/**
	 * sends request to the correct server to get the key and the value
	 * 
	 * @param key
	 *            the key to be selected
	 * 
	 * @return KVMessage represents the result of this operation
	 */
	@Override
	public KVMessage get(String key) throws IOException {
		ClientMessage msg = new ClientMessage();
		KVMessage receivedMsg = null;
		msg.setKey(key);
		msg.setStatus(KVMessage.StatusType.GET);
		// first check the local cache
		if(dataStoreCache.get(key) != null){
			String value = dataStoreCache.get(key);
			logger.info("data retrieved from local cache of subscribed keys!");
			msg.setStatus(KVMessage.StatusType.GET_SUCCESS);
			msg.setValue(value);
			return msg;
		}
		
		// ServerInfo tempInfo = this.getDestinationServerInfo ( key );
		try {
			/*
			 * if ( ! this.currentDestinationServer.equals ( tempInfo ) ) {
			 * this.switchConnection ( tempInfo ); }
			 */
			this.sendMessage(msg);
			receivedMsg = this.receiveMessage();

		} catch (IOException e) {
			logger.error("error in sending or receiving message");
		}
		return receivedMsg;
	}

	private KVMessage receiveMessage() throws IOException {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;

		while (read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				if (msgBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			/* only read valid characters, i.e. letters and numbers */
			if ((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}

			/* stop reading is DROP_SIZE is reached */
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}

			/* read next char from stream */
			read = (byte) input.read();
		}

		if (msgBytes == null) {
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}

		msgBytes = tmp;

		/* build final String */
		KVMessage msg = (KVMessage) SerializationUtil.toObject(msgBytes);
		logger.info("Receive message:\t '" + msg.toString() + "'");
		return msg;
	}

	private void sendMessage(AbstractMessage msg) throws IOException {
		this.lastSentMessage = msg;
		byte[] msgBytes = null;
		if (msg.getMessageType() == MessageType.CLIENT_MESSAGE) {
			msgBytes = SerializationUtil.toByteArray((ClientMessage) msg);
		} else if (msg.getMessageType() == MessageType.SUBSCRIBE_MESSAGE) {
			msgBytes = SerializationUtil.toByteArray((SubscribeMessage) msg);
		}else if (msg.getMessageType() == MessageType.UNSUBSCRIBE_MESSAGE) {
			msgBytes = SerializationUtil.toByteArray((UnsubscribeMessage) msg);
		}

		if (msgBytes != null) {
			output.write(msgBytes, 0, msgBytes.length);
			output.flush();
			logger.info("Send message :\t '" + msg.toString() + "'");
		}
	}
	/**
	 * updates the metadata with a new copy
	 * 
	 * @param metadata
	 *            the new metadata to be settled
	 */
	public void updateMetadata(List<ServerInfo> metadata) {
		List<ServerInfo> oldMetaData = new ArrayList<ServerInfo>();
		List<ServerInfo> suspectedServers;
		List<String> keys;
		Map<ServerInfo,List<String>> tobeSubscribed;
		
		//making a deep copy of the needed informations
		if(this.metadata != null){
		/*	for(ServerInfo s: this.metadata){
				ServerInfo temp = new ServerInfo();
				temp.setAddress(s.getAddress());
				temp.setPort(s.getPort());
				temp.setFromIndex(s.getFromIndex());
				temp.setToIndex(s.getToIndex());
				temp.setSecondCoordinatorInfo(s.getSecondCoordinatorInfo());
				oldMetaData.add(temp);
			}*/
			oldMetaData = this.metadata;
		}
		logger.debug("OLD META DATA");
		logger.debug(oldMetaData);
		
		//copying new metaData
		this.metadata = metadata;
		
		// metadata sent to the client does not have all the info
		for(ServerInfo s: this.metadata){
			s.setFirstCoordinatorInfo( metadata.get((metadata.indexOf(s) + metadata.size() -2 )% metadata.size()));
			s.setSecondCoordinatorInfo(metadata.get((metadata.indexOf(s) + metadata.size() -1 )% metadata.size()));
		}
		
		suspectedServers = checkSubscribtionValidation(oldMetaData);
		
		logger.debug("suspected " );
		logger.debug(suspectedServers);
		
		keys = checkKeyResponsibilities(suspectedServers);
		logger.debug("affected subscribed keys !");
		logger.debug(keys);
		
		tobeSubscribed = sortKeysByServer(keys);
		
		logger.debug(tobeSubscribed);

		updateSubscription(tobeSubscribed);
		dataStoreCache.cleanServerList(metadata);
		logger.info("update metadata with " + metadata.size() + " keys");
	}
	
	/**
	 * checks if a servers responsiblity may have been changed or not (ownership of keys)
	 * @param oldMetaData
	 * @return the list of the servers which are suspected
	 */
	public List<ServerInfo> checkSubscribtionValidation(List<ServerInfo> oldMetaData){
		if(oldMetaData.isEmpty())
			return null;
		List<ServerInfo> toBeChecked = new ArrayList<ServerInfo>();
		for(ServerInfo s:oldMetaData){
			if(this.metadata.contains(s)){
				int newSIndex = this.metadata.indexOf(s);
				ServerInfo newS = this.metadata.get(newSIndex);
				/* check their closest master( coordinator(1))
				* if it has changed, Server s may has lost some
				* keys
				*/
				
				if(! (s.getSecondCoordinatorInfo().equals(newS.getSecondCoordinatorInfo()))){
					toBeChecked.add(newS);
					
				}
			}else{
				// s has been removed from the system, check the keys!
				toBeChecked.add(s);
			}
		}
		return toBeChecked;
	}
	
	/**
	 * checks from dataStoreCache subscribtion list, which server has to be checks.
	 * Then check this server's keys and if the server has lost responsibility of that
	 * key, key is added to toBeSubscribe list of keys
	 * @param toBechecked
	 * @return list of toBesubscribekeys
	 */
	public List<String> checkKeyResponsibilities(List<ServerInfo> toBechecked){
		if(toBechecked == null || toBechecked.isEmpty())
			return null;
		List<String> toBesubscribekeys = new ArrayList<String>();
		List<ServerInfo> subscribedServers = dataStoreCache.getSubscribedServers();
		if(subscribedServers != null || ! subscribedServers.isEmpty()){
			for(ServerInfo subscribed:subscribedServers){
				if (toBechecked.contains(subscribed)){
					List<String> keys = dataStoreCache.getSubscribedKeys(subscribed);
					for(String key: keys){
						// if the server has lost the ownership of the key
						if(! getDestinationServerInfo(key).equals(subscribed))
							toBesubscribekeys.add(key);
					}
				}
			}
		}
		return toBesubscribekeys;
	}
	
	/**
	 * sorts keys by each server.
	 * @param keys
	 * @return Map of serverInfo, keys each ServerInfo is mapped to list of its keys
	 */
	public Map<ServerInfo, List<String>> sortKeysByServer(List<String> keys){
		if(keys == null || keys.isEmpty())
			return null;
		Map<ServerInfo, List<String>> keysByServer = new HashMap<ServerInfo, List<String>>(); 
		List<String> temp = new ArrayList<String>();
		for(String key:keys){
			ServerInfo responsible = getDestinationServerInfo(key);
			if(keysByServer.containsKey(responsible)){
				temp = keysByServer.get(responsible);
				temp.add(key);
				keysByServer.put(responsible, temp);
			}else{
				temp = new ArrayList<String>();
				temp.add(key);
				keysByServer.put(responsible, temp);
			}
		}
		return keysByServer;
	}

	/**
	 * tries to update subscriptions for the keys' which their mainserver has changed
	 * @param tobeSubscribeAgain
	 */
	public void updateSubscription(Map<ServerInfo, List<String>> tobeSubscribeAgain){
		if(tobeSubscribeAgain == null || tobeSubscribeAgain.isEmpty())
			return;
		// first send subscription messages to the current connection if possible
		List<String> tempList;
		if(tobeSubscribeAgain.containsKey(this.getCurrentConnection())){
			tempList = tobeSubscribeAgain.get(currentDestinationServer);
			for(String key:tempList){
				this.getS(key,paretnInfo );
			}
			tobeSubscribeAgain.remove(this.getCurrentConnection());
		}
		
		ServerInfo prevConnection = this.getCurrentConnection();
		
		// send subscription to the other servers!
		for(ServerInfo s: tobeSubscribeAgain.keySet()){
			tempList = tobeSubscribeAgain.get(s);
			for(String key: tempList){
				KVMessage res = this.getS(key, paretnInfo);
				subscribe(s,res.getKey(),res.getValue());
			}
		}
		
		//turn back the connection to the previous connection (before starting subscription updates)
		try {
			this.switchConnection ( prevConnection );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public AbstractMessage getLastSentMessage() {
		return this.lastSentMessage;
	}

	public ServerInfo getDestinationServerInfo(String key) {
		ServerInfo destinationServer = null;
		Hasher hasher = new Hasher();
		if (metadata.size() != 0) {

			for (ServerInfo server : metadata) {
				if (hasher.isInRange(server.getFromIndex(),
						server.getToIndex(), key)) {

					/*
					 * logger.debug("responsible server for " +
					 * hasher.getHash(key) + " is " + server.getFromIndex() +
					 * "  " + server.getToIndex());
					 */
					destinationServer = server;
				}
			}

		} else {
			destinationServer = this.currentDestinationServer;
			logger.info("metadata is empty !!");
		}

		return destinationServer;
	}

	public ServerInfo getCurrentConnection() {
		return this.currentDestinationServer;
	}

	public KVMessage getS(String key,ClientInfo clientInfo) {
		SubscribeMessage msg = new SubscribeMessage();
		KVMessage receivedMsg = null;
		msg.setKey(key);
		msg.setSubscriber(clientInfo);
		msg.setStatusType(StatusType.GET);
		ServerInfo tempInfo = this.getDestinationServerInfo ( key );
		try {
			 if ( ! this.currentDestinationServer.equals ( tempInfo ) )
			 this.switchConnection ( tempInfo ); 
			this.sendMessage(msg);
			receivedMsg = this.receiveMessage();
			//dataStoreCache.push(key, receivedMsg.getValue());
		} catch (IOException e) {
			logger.error("error in sending or receiving message");
		}
		return receivedMsg;
	}

public KVMessage putS(String key, String value,ClientInfo clientInfo) {
	SubscribeMessage msg = new SubscribeMessage();
	KVMessage receivedMsg = null;
	msg.setKey(key);
	msg.setValue(value);
	msg.setSubscriber(clientInfo);
	msg.setStatusType(StatusType.PUT);
	ServerInfo tempInfo = this.getDestinationServerInfo (key);
	try {
		 if ( ! this.currentDestinationServer.equals ( tempInfo ) )
		 this.switchConnection ( tempInfo ); 
		this.sendMessage(msg);
		receivedMsg = this.receiveMessage();
		//dataStoreCache.push(key, receivedMsg.getValue());
	} catch (IOException e) {
		logger.error("error in sending or receiving message");
	}
	return receivedMsg;
}

public void subscribe(ServerInfo server, String key, String value){
	dataStoreCache.subscribetoServer(server, key, value);
}

public KVMessage unsubscribe(String key, ClientInfo clientInfo) {
	KVMessage receivedMsg = null;
	if(dataStoreCache.get(key) == null)
		return receivedMsg;
	logger.debug("unsubscribing from " + key);
	
	ServerInfo responsible = getDestinationServerInfo(key);
	dataStoreCache.remove(key, responsible);
	logger.debug("key has been removed from local catch");
	UnsubscribeMessage msg = new UnsubscribeMessage();
	msg.setKey(key);
	msg.setSubscriber(clientInfo);
	try {
		 if ( ! this.currentDestinationServer.equals ( responsible ) )
			 this.switchConnection ( responsible ); 
		logger.debug("before send message");
		this.sendMessage(msg);
		receivedMsg = this.receiveMessage();
		logger.debug(receivedMsg);

	} catch (IOException e) {
		logger.error("error in sending or receiving message");
	}
	return receivedMsg;
}

	public void updateCache(String key, String value){
		dataStoreCache.push(key, value);
	}

/* unsibscribe keyList version! Not yet complete
public ArrayList<String> unsubscribe(String[] keys, ClientInfo clientInfo) {
	//SubscribeMessage msg = new UnSubscribeMessage();
	KVMessage receivedMsg = null;
	ArrayList<String> response = new ArrayList<String>();
	HashMap<ServerInfo, ArrayList<String>> responsibleServers = new HashMap<ServerInfo, ArrayList<String>>();
	// each responsible server will have a list of its responsible keys
	for(String key:keys){
		ServerInfo tempInfo = this.getDestinationServerInfo (key);
		if(responsibleServers.containsKey(tempInfo)){
			ArrayList<String> temp = responsibleServers.get(tempInfo);
			temp.add(key);
			responsibleServers.put(tempInfo, temp);
		}else{
			ArrayList<String>temp = new ArrayList<String>();
			temp.add(key);
			responsibleServers.put(tempInfo, temp);
		}
	}
	for(ServerInfo s:responsibleServers.keySet()){
		//TODO @Ibrahim requirements of unsubscribe msg type 
		/*msg.setKeys(responsibleServers.get(s));
		msg.setStatusType(StatusType.UNSUBSCRIBE);
		msg.setUnSubscriber(clientInfo);
		try { 
			this.sendMessage(msg);
			receivedMsg = this.receiveMessage();
			if(receivedMsg.getStatus().equals(StatusType.UNSUBSCRIBE_SUCCESS))
				for(String key: responsibleServers.get(s))
					response.add(key);
		} catch (IOException e) {
			logger.error("error in sending or receiving message");
		}
	}
	}
	return response;
}*/
	public List<ServerInfo> getMetaData(){
		return this.metadata;
	}
	
	public void sMetaData(List<ServerInfo>metaData){
		this.metadata = metaData;
		for(ServerInfo s: this.metadata){
			s.setFirstCoordinatorInfo( metadata.get((metadata.indexOf(s) + metadata.size() -2 )% metadata.size()));
			s.setSecondCoordinatorInfo(metadata.get((metadata.indexOf(s) + metadata.size() -1 )% metadata.size()));
		}
	}
	
	public SubscribtionCache getSubscription(){
		return this.dataStoreCache;
	}
}