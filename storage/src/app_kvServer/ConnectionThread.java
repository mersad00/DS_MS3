/**
 * This class is the thread for every new connection
 * to the server, containing all the required methods
 * to handle the requests, there is 3 types of new 
 * connection to the server:
 * <ul>
 * <li>client connection and it is recognized by the 
 * <code>ClientMessage</code></li>
 * <li>server connection and it is recognized by the 
 * <code>ServerMessage</code></li>
 * <li>ECS connection and it is recognized by the 
 * <code>ECSMessage</code></li>
 * </ul>
 * This class receives the new message and recognize the 
 * type of the new message and then choose the appropriate 
 * action.
 * 
 * 
 * @see ClientMessage
 * @see ServerMessage
 * @see ECSMessage
 * @see ServerStatuses
 * 
 */
package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;

import javax.activation.UnsupportedDataTypeException;

import org.apache.log4j.Logger;

import utilities.LoggingManager;
import app_kvEcs.ECSCommand;
import app_kvEcs.ECSMessage;
import app_kvEcs.RecoverMessage;
import client.SerializationUtil;
import common.Hasher;
import common.ServerInfo;
import common.messages.AbstractMessage;
import common.messages.KVMessage;
import common.messages.ClientMessage;
import common.messages.AbstractMessage.MessageType;
import common.messages.KVMessage.StatusType;
import common.messages.SubscribeMessage;
import common.messages.UnsubscribeMessage;

public class ConnectionThread implements Runnable {

	private static Logger logger;

	private boolean isOpen;

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	private KVServer parent;
	private ECSMessage dataToBeRemoved;
	private DatabaseManager dbManager;
	private DatabaseManager rep1;
	private DatabaseManager rep2;


	/**
	 * Constructs a new <code>ConnectionThread</code> object for a given TCP
	 * socket.
	 * 
	 * @param clientSocket
	 *            the Socket object for the client connection.
	 * @param parent
	 *            the server object which is listening to new connections
	 */
	public ConnectionThread(Socket clientSocket, KVServer parent,
			DatabaseManager db, DatabaseManager rep1, DatabaseManager rep2,SubscriberStorageManager subscriberStorageManager) {
		this.parent = parent;
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.dbManager = db;
		this.rep1 = rep1;
		this.rep2 = rep2;
		logger = LoggingManager.getInstance().createLogger(this.getClass());
	}

	/**
	 * Initializes and starts the connection. Loops until the connection is
	 * closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();

			while (isOpen) {
				try {
					AbstractMessage msg = receiveMessage();
					handleRequest(msg); // to determine the connection type
					/*
					 * connection either terminated by the client or lost due to
					 * network problems
					 */
				} catch (IOException ioe) {
					/*
					 * logger.error("Error! Connection lost!" +
					 * ioe.getStackTrace());
					 */
					isOpen = false;
				}
			}

		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);

		} finally {

			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

	/**
	 * receives the sent bytes and de-serialize into Object
	 * 
	 * @return <code>AbstractMessage</code> general type of message.
	 * @throws IOException
	 */
	private AbstractMessage receiveMessage() throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;
		if (read == -1) {
			throw new IOException();
		}
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

		/* build final Message */
		AbstractMessage msg = SerializationUtil.toObject(msgBytes);
		// logger.info("Receive message:\t '" + msg.getMessageType() + "'");
		return msg;
	}

	/**
	 * the low level sending method
	 * 
	 * @param msgBytes
	 *            the serialized message
	 * @throws IOException
	 */
	private void sendMessage(byte[] msgBytes) throws IOException {
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
	}

	/**
	 * sends message of type <code>KVMessage</code>
	 * 
	 * @param msg
	 *            the <code>KVMessage</code> object
	 * @throws IOException
	 */
	private void sendClientMessage(KVMessage msg) throws IOException {
		byte[] msgBytes = SerializationUtil.toByteArray((ClientMessage) msg);
		sendMessage(msgBytes);
		logger.info("Send client message:\t '" + msg.getStatus() + "'");
	}

	/**
	 * sends message of type <code>ServerMessage</code>
	 * 
	 * @param msg
	 *            the <code>ServerMessage</code> object
	 * @throws IOException
	 */
	private void sendServerMessage(ServerMessage msg, ServerInfo server) {
		byte[] msgBytes = SerializationUtil.toByteArray(msg);
		Socket connectionToOtherServer = null;
		OutputStream output = null;
		try {
			logger.debug("message " + msg.getSaveFromIndex() + ":"
					+ msg.getSaveToIndex() + " size: " + msg.getData().size()
					+ " is send to server" + server.getPort());
			connectionToOtherServer = new Socket(server.getAddress(),
					server.getPort());
			output = connectionToOtherServer.getOutputStream();
			output.write(msgBytes, 0, msgBytes.length);
			output.flush();
		} catch (UnknownHostException e) {
			logger.error("Error in moving data to server : "
					+ server.toString() + "Can not find server ");
		} catch (IOException e) {
			logger.error("Error in moving data to server : "
					+ server.toString() + "Can not make connection ");
		} finally {
			try {
				if (output != null && connectionToOtherServer != null) {
					output.close();
					connectionToOtherServer.close();
				}
			} catch (IOException e) {
				logger.error("Error in moving data to server : "
						+ server.toString() + "Can not close connection ");
			}

		}

		logger.info("data moved to '" + server.toString() + "'");
	}

	/**
	 * sends message of type <code>ECSMessage</code>
	 * 
	 * @param msg
	 *            the <code>ECSMessage</code> object
	 * @throws IOException
	 */
	private void sendECSMessage(ECSMessage msg) throws IOException {
		byte[] msgBytes = SerializationUtil.toByteArray(msg);
		this.sendMessage(msgBytes);
	}

	private void sendReplicationMessage(ReplicaMessage msg, ServerInfo server) {
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
			logger.error("Error in replicating data : " + server.toString()
					+ "Can not find server ");
		} catch (IOException e) {
			logger.error("Error in Error in replicating data : "
					+ server.toString() + "Can not make connection ");
		} finally {
			try {
				if (output != null && connectionToOtherServer != null) {
					output.close();
					connectionToOtherServer.close();
				}
			} catch (IOException e) {
				logger.error("Error in replicating data : " + server.toString()
						+ "Can not close connection ");
			}

		}

		logger.info("data replicated to '" + server.toString() + "'");
	}

	/**
	 * 
	 * @param msg
	 *            of type <code>AbstractMessage</code> determine the received
	 *            message type and select the appropriate action
	 * @throws IOException
	 */
	private void handleRequest(AbstractMessage msg) throws IOException {
		if (msg.getMessageType().equals(MessageType.CLIENT_MESSAGE)) {
			handleClientRequest((ClientMessage) msg);
		} else if (msg.getMessageType().equals(MessageType.SERVER_MESSAGE)) {
			handleServerRequest((ServerMessage) msg);
		} else if (msg.getMessageType().equals(MessageType.ECS_MESSAGE)) {
			handleECSRequest((ECSMessage) msg);
		} else if (msg.getMessageType().equals(MessageType.REPLICA_MESSAGE)) {
			handleReplicationRequest((ReplicaMessage) msg);
		} else if (msg.getMessageType().equals(MessageType.HEARTBEAT_MESSAGE)) {
			handleHeartbeatRequest((HeartbeatMessage) msg);
		} else if (msg.getMessageType().equals(MessageType.RECOVERY_MESSAGE)) {
			logger.debug("GOT A RECOVERY MESSAGE");
			handleRecoverDataRequest((RecoverMessage) msg);
		} else if (msg.getMessageType().equals(MessageType.SUBSCRIBE_MESSAGE)) {
			handleSubscribeMessage((SubscribeMessage)msg);
		} else if (msg.getMessageType().equals(MessageType.SUBSCRIBE_MESSAGE)) {
			handleUnsubscribeMessage((UnsubscribeMessage)msg);
		}
		else
			logger.debug("VAGUE MSG RECEIVED");
	}
	
	private void handleUnsubscribeMessage(UnsubscribeMessage msg) throws IOException{		
		KVMessage responseMessage = null;
		if (parent.getServerStatus()
				.equals(ServerStatuses.UNDER_INITIALIZATION)
				|| parent.getServerStatus().equals(ServerStatuses.STOPPED)) {

			/*
			 * The server has just started and not ready yet to handle requests
			 * from clients
			 */
			responseMessage = new ClientMessage();
			((ClientMessage)responseMessage).setKey(msg.getKey());
			((ClientMessage)responseMessage).setStatus(StatusType.SERVER_STOPPED);
			
		} else if (parent.getServerStatus().equals(ServerStatuses.WRITING_LOCK)) {

			/* The server is locked for writing operations */			
			responseMessage = new ClientMessage();
			((ClientMessage)responseMessage).setKey(msg.getKey());
			((ClientMessage)responseMessage).setStatus(StatusType.SERVER_WRITE_LOCK);

		}

		/* check if the received message is in this server range */
		else if (isInMyRange(msg.getKey())) {			
			this.parent.removeSubscriber(msg.getKey(), msg.getSubscriber());
			responseMessage = new ClientMessage();
			((ClientMessage)responseMessage).setKey(msg.getKey());
			((ClientMessage)responseMessage).setStatus(StatusType.UNSUBSCRIBE_SUCCESS);			
			
		}  else {
			/* in case the received message is in the range of this server */
			responseMessage = new ClientMessage();
			((ClientMessage)responseMessage).setKey(msg.getKey());
			((ClientMessage)responseMessage).setStatus(StatusType.SERVER_NOT_RESPONSIBLE);			
			((ClientMessage) responseMessage).setMetadata(parent.getMetadata());
		}

		this.sendClientMessage(responseMessage);
		logger.info("response message sent to client ");
		
	}

	
	private void handleSubscribeMessage(SubscribeMessage msg) throws IOException{		
		KVMessage responseMessage = null;
		if (parent.getServerStatus()
				.equals(ServerStatuses.UNDER_INITIALIZATION)
				|| parent.getServerStatus().equals(ServerStatuses.STOPPED)) {

			/*
			 * The server has just started and not ready yet to handle requests
			 * from clients
			 */
			msg.setStatusType(StatusType.SERVER_STOPPED);
			responseMessage = new ClientMessage(msg);

		} else if (parent.getServerStatus().equals(ServerStatuses.WRITING_LOCK)) {

			/* The server is locked for writing operations */
			msg.setStatusType(StatusType.SERVER_WRITE_LOCK);
			responseMessage = new ClientMessage(msg);

		}

		/* check if the received message is in this server range */

		else if (isInMyRange(msg.getKey())
				&& msg.getStatusType().equals(KVMessage.StatusType.PUT)) {
			/*
			 * responseMessage = DatabaseManager.put ( msg.getKey () ,
			 * msg.getValue () );
			 */
			responseMessage = dbManager.put(msg.getKey(), msg.getValue());
			parent.NotifyIfHasSubscriber(msg.getKey(), msg.getValue());
			
			this.parent.addSubscriber(msg.getKey(), msg.getSubscriber());
			ReplicaMessage replicationMessage = new ReplicaMessage();
			replicationMessage.setCoordinatorServer(parent.getThisServerInfo());

			replicationMessage.setKey(msg.getKey());
			replicationMessage.setValue(msg.getValue());
			replicationMessage.setStatusType(StatusType.PUT);
			logger.debug("replication message prepared ");
			sendReplicationMessage(replicationMessage, replicationMessage
					.getCoordinatorServerInfo().getFirstReplicaInfo());
			sendReplicationMessage(replicationMessage, replicationMessage
					.getCoordinatorServerInfo().getSecondReplicaInfo());
			

		} else if (msg.getStatusType().equals(KVMessage.StatusType.GET)) {
			if (isInMyRange(msg.getKey())) {
				// responseMessage = DatabaseManager.get ( msg.getKey () );
				responseMessage = dbManager.get(msg.getKey());
				this.parent.addSubscriber(msg.getKey(), msg.getSubscriber());
			} else {
				// /check if replica(s) storages have the key
				KVMessage replicaServeGet = serveIfInMyReplicaRange(msg
						.getKey());
				if (replicaServeGet != null) {
					responseMessage = replicaServeGet;
				} else {
					// / this is a get request which neither server nor replicas
					// storages
					// / have the data, therefore reply not responsible
					msg.setStatusType(StatusType.SERVER_NOT_RESPONSIBLE);
					responseMessage = new ClientMessage(msg);
					((ClientMessage) responseMessage).setMetadata(parent
							.getMetadata());
				}
			}

		} else {
			/* in case the received message is in the range of this server */
			msg.setStatusType(StatusType.SERVER_NOT_RESPONSIBLE);
			responseMessage = new ClientMessage(msg);
			((ClientMessage) responseMessage).setMetadata(parent.getMetadata());
		}

		this.sendClientMessage(responseMessage);
		logger.info("response message sent to client ");
	}

	private void handleHeartbeatRequest(HeartbeatMessage msg) {
		/*
		 * logger.debug("Handle HeartBeat(" +
		 * parent.getThisServerInfo().getPort() + ") =>[" +
		 * msg.getCoordinatorServer().getPort() + "] C1{" +
		 * parent.getThisServerInfo().getFirstCoordinatorInfo().getPort()+"}C2{"
		 * +
		 * parent.getThisServerInfo().getSecondCoordinatorInfo().getPort()+"}");
		 */
		if (msg.getCoordinatorServer().equals(
				parent.getThisServerInfo().getFirstCoordinatorInfo())) {
			parent.setFirstCoordinatorLastSeen(new Date());
		} else if (msg.getCoordinatorServer().equals(
				parent.getThisServerInfo().getSecondCoordinatorInfo())) {
			parent.setSecondCoordinatorLastSeen(new Date());
		}
	}

	/**
	 * in case of the received message is a client request
	 * 
	 * @param msg
	 *            of type <code>ClientMessage</code>
	 * @throws IOException
	 */
	private void handleClientRequest(ClientMessage msg) throws IOException {
		KVMessage responseMessage = null;
		if (parent.getServerStatus()
				.equals(ServerStatuses.UNDER_INITIALIZATION)
				|| parent.getServerStatus().equals(ServerStatuses.STOPPED)) {

			/*
			 * The server has just started and not ready yet to handle requests
			 * from clients
			 */
			msg.setStatus(StatusType.SERVER_STOPPED);
			responseMessage = new ClientMessage(msg);

		} else if (parent.getServerStatus().equals(ServerStatuses.WRITING_LOCK)) {

			/* The server is locked for writing operations */
			msg.setStatus(StatusType.SERVER_WRITE_LOCK);
			responseMessage = new ClientMessage(msg);

		}
		//TODO handle PutS and GetS and Unsibscribe messages with proper reponse

		/* check if the received message is in this server range */

		else if (isInMyRange(msg.getKey())
				&& msg.getStatus().equals(KVMessage.StatusType.PUT)) {
			/*
			 * responseMessage = DatabaseManager.put ( msg.getKey () ,
			 * msg.getValue () );
			 */
			responseMessage = dbManager.put(msg.getKey(), msg.getValue());
			parent.NotifyIfHasSubscriber(msg.getKey(), msg.getValue());
			
			ReplicaMessage replicationMessage = new ReplicaMessage();
			replicationMessage.setCoordinatorServer(parent.getThisServerInfo());

			replicationMessage.setKey(msg.getKey());
			replicationMessage.setValue(msg.getValue());
			replicationMessage.setStatusType(StatusType.PUT);
			logger.debug("replication message prepared ");
			sendReplicationMessage(replicationMessage, replicationMessage
					.getCoordinatorServerInfo().getFirstReplicaInfo());
			sendReplicationMessage(replicationMessage, replicationMessage
					.getCoordinatorServerInfo().getSecondReplicaInfo());

		} else if (msg.getStatus().equals(KVMessage.StatusType.GET)) {
			if (isInMyRange(msg.getKey())) {
				// responseMessage = DatabaseManager.get ( msg.getKey () );
				responseMessage = dbManager.get(msg.getKey());
			} else {
				// /check if replica(s) storages have the key
				KVMessage replicaServeGet = serveIfInMyReplicaRange(msg
						.getKey());
				if (replicaServeGet != null) {
					responseMessage = replicaServeGet;
				} else {
					// / this is a get request which neither server nor replicas
					// storages
					// / have the data, therefore reply not responsible
					msg.setStatus(StatusType.SERVER_NOT_RESPONSIBLE);
					responseMessage = new ClientMessage(msg);
					((ClientMessage) responseMessage).setMetadata(parent
							.getMetadata());
				}
			}

		} else {
			/* in case the received message is in the range of this server */
			msg.setStatus(StatusType.SERVER_NOT_RESPONSIBLE);
			responseMessage = new ClientMessage(msg);
			((ClientMessage) responseMessage).setMetadata(parent.getMetadata());
		}

		this.sendClientMessage(responseMessage);
		logger.info("response message sent to client ");
	}

	private boolean isInMyRange(String key) {
		Hasher hasher = new Hasher();
		String startRange = parent.getThisServerInfo().getFromIndex();
		String endRange = parent.getThisServerInfo().getToIndex();
		String keyHash = hasher.getHash(key);

		boolean b = hasher.isInRange(startRange, endRange, key);
		if (b)
			logger.debug(keyHash + " is in Range: " + startRange + " : "
					+ endRange);
		else
			logger.debug(keyHash + " is NOT in Range: " + startRange + " : "
					+ endRange);
		return b;
	}

	private KVMessage serveIfInMyReplicaRange(String key) {

		ServerInfo responsibleServer = null;
		Hasher hasher = new Hasher();
		if (parent.getMetadata().size() != 0) {

			for (ServerInfo server : parent.getMetadata()) {
				if (hasher.isInRange(server.getFromIndex(),
						server.getToIndex(), key)) {
					logger.debug("Requested key : " + hasher.getHash(key)
							+ "is in the resposibility of the server:" + server);
					responsibleServer = server;
				}
			}
		}
		if (responsibleServer != null) {
			if (parent.getThisServerInfo().getFirstCoordinatorInfo()
					.equals(responsibleServer)) {
				logger.debug("Requested key is in the replica database of server:"
						+ parent.getThisServerInfo()
						+ " replication of:"
						+ responsibleServer);
				return rep1.get(key);
			} else if (parent.getThisServerInfo().getSecondCoordinatorInfo()
					.equals(responsibleServer)) {
				logger.debug("Requested key is in the replica database of server:"
						+ parent.getThisServerInfo()
						+ " replication of:"
						+ responsibleServer);
				return rep2.get(key);
			}
		}
		return null;
	}

	/**
	 * in case of the received message is a server request this method is only
	 * for moving data to this server (also getting replicated data from other
	 * servers)
	 * 
	 * @param msg
	 *            of type <code>ServerMessage</code>
	 * @throws IOException
	 */
	private void handleServerRequest(ServerMessage msg) throws IOException {
		logger.debug("*******************SERVER MESSAGE RECIEVED*********************");
		if (msg.getData().size() > 0) {
			String fromIndex = msg.getSaveFromIndex();
			String toIndex = msg.getSaveToIndex();
			logger.debug("RECIEVED DATA FROM OTHER SERVERS");
			for (String s : msg.getData().keySet())
				logger.debug("RECIEVED DATA: " + s);
			// DatabaseManager.putAll ( msg.getData () );
			if (isIndexInRange(fromIndex, toIndex, parent.getThisServerInfo())) {
				dbManager.putAll(msg.getData());
				logger.info("updated database with : \t" + msg.getData().size()
						+ " keys ");
				logger.debug("$$$$$ PRINTING DATABASE $$$$$");
				dbManager.printDatabase();
				return;
			} else if (isIndexInRange(fromIndex, toIndex, parent
					.getThisServerInfo().getFirstCoordinatorInfo())) {
				rep1.putAll(msg.getData());
				logger.info("updated Replica1 with : \t" + msg.getData().size()
						+ " keys ");
				logger.debug("$$$$$ PRINTING REPLICA1 $$$$$");
				rep1.printDatabase();

				return;
			} else if (isIndexInRange(fromIndex, toIndex, parent
					.getThisServerInfo().getSecondCoordinatorInfo())) {
				rep2.putAll(msg.getData());
				logger.info("updated Replica2  with : \t"
						+ msg.getData().size() + " keys ");
				logger.debug("$$$$$ PRINTING REPLICA2 $$$$$");
				rep2.printDatabase();

				return;
			}

			logger.debug("SERVER MESSAGE WAS VAGUE!");
		}
		logger.debug("SERVER MESSAGE WAS EMPTY");
	}

	/**
	 * in case of the received message is a ECS command
	 * 
	 * @param msg
	 *            of type <code>ECSMessage</code>
	 * @throws IOException
	 */
	private void handleECSRequest(ECSMessage msg) throws IOException {
		if (msg.getActionType().equals(ECSCommand.INIT)) {
			logger.info("server is initializing ...");
			parent.setMetadata(msg.getMetaData());
			parent.setServerStatus(ServerStatuses.UNDER_INITIALIZATION);

		} else if (msg.getActionType().equals(ECSCommand.START)) {
			logger.info("server is starting ...");
			parent.setServerStatus(ServerStatuses.ACTIVE);

		} else if (msg.getActionType().equals(ECSCommand.STOP)) {
			logger.info("server is stopping ... ");
			parent.setServerStatus(ServerStatuses.STOPPED);

		} else if (msg.getActionType().equals(ECSCommand.SHUT_DOWN)) {
			parent.shutdown();

		} else if (msg.getActionType().equals(ECSCommand.SET_WRITE_LOCK)) {
			logger.info("locking server for write ...");
			parent.setServerStatus(ServerStatuses.WRITING_LOCK);

		} else if (msg.getActionType().equals(ECSCommand.RELEASE_LOCK)) {
			logger.info("releasing writing lock ...");
			logger.info("remove data from server ");
			if (this.dataToBeRemoved != null) {
				dbManager.removeDataInRange(
						this.dataToBeRemoved.getMoveFromIndex(),
						this.dataToBeRemoved.getMoveToIndex());
			}
			parent.setServerStatus(ServerStatuses.ACTIVE);

		} else if (msg.getActionType().equals(ECSCommand.MOVE_DATA)) {
			logger.info("preparing data to be moved ... ");
			ServerMessage message = new ServerMessage();

			// Moving whole storage for replication! (not deleting the data
			// afterwards)
			logger.debug("MOVE DATA MSG RECIEVED range: "
					+ msg.getMoveFromIndex() + " " + msg.getMoveToIndex());

			logger.debug("this server's range: "
					+ parent.getThisServerInfo().getFromIndex() + " "
					+ parent.getThisServerInfo().getToIndex());

			message.setData(dbManager.getDataInRange(msg.getMoveFromIndex(),
					msg.getMoveToIndex()));
			message.setSaveFromIndex(msg.getMoveFromIndex());
			message.setSaveToIndex(msg.getMoveToIndex());

			if (msg.getMoveFromIndex().equals(
					parent.getThisServerInfo().getFromIndex())
					&& msg.getMoveToIndex().equals(
							parent.getThisServerInfo().getToIndex())) {
				logger.debug("*************************\n "
						+ "sending own storage for replication to "
						+ msg.getMoveToServer());
			} else {
				this.dataToBeRemoved = msg;
				logger.debug("**************************\n"
						+ "moving own storage to " + msg.getMoveToServer());
				dbManager.removeDataInRange(
						this.dataToBeRemoved.getMoveFromIndex(),
						this.dataToBeRemoved.getMoveToIndex());
				logger.debug("data set to move has been removed from dataBase...");
				this.dataToBeRemoved = null;

			}

			this.sendServerMessage(message, msg.getMoveToServer());

			/*
			 * if (message.getData().keySet().size() > 0) {
			 * this.sendServerMessage(message, msg.getMoveToServer());
			 * logger.info("data moved to : " +
			 * msg.getMoveToServer().toString());
			 * 
			 * } else logger.info(
			 * "No data stored yet ! Database is empty. moving data aborted" +
			 * msg.getMoveToServer().toString());
			 */
			ECSMessage acknowledgeMessage = new ECSMessage();
			acknowledgeMessage.setActionType(ECSCommand.ACK);
			this.sendECSMessage(acknowledgeMessage);
			logger.info("send acknowledgment back to  ECS");

		} else if (msg.getActionType().equals(ECSCommand.REMOVE_DATA)) {
			/*
			 * sent by ECS to remove replicated data which was from the old
			 * master the old master's replicated data is on rep1 remove the
			 * data belonged to old master(0) from rep1 swap the data belong to
			 * old master(1) from rep2 to rep1. Because now old master(1) is new
			 * master(0)
			 */
			logger.info("preparing data to be removed ... ");
			String fromIndex = msg.getMoveFromIndex();
			String toIndex = msg.getMoveToIndex();
			logger.info("removing data form first replica storage from "
					+ fromIndex + " to: " + toIndex);
			rep1.removeDataInRange(fromIndex, toIndex);

			// skip swapping if the ex coordinator(1) is the new coordinator(1)
			// nothing changed just
			// the new coordinator(0) is new!
			if (!parent.getThisServerInfo().getSecondCoordinatorInfo()
					.equals(parent.getExCoordinator(1))) {
				logger.debug("swaping data from rep2 (old master(1) which is now (new master(0)) to rep1 ");
				// when the excoordinator(1) is the newcoordinator(0)
				// meta data is updated so swapping first coordinator from rep2,
				// which was the
				// second coordinator
				rep1.putAll(rep2.getDataInRange(parent.getThisServerInfo()
						.getFirstCoordinatorInfo().getFromIndex(), parent
						.getThisServerInfo().getFirstCoordinatorInfo()
						.getToIndex()));

				rep2.removeDataInRange(parent.getThisServerInfo()
						.getFirstCoordinatorInfo().getFromIndex(), parent
						.getThisServerInfo().getFirstCoordinatorInfo()
						.getToIndex());
			} else if (parent.getThisServerInfo().getFirstCoordinatorInfo()
					.equals(parent.getExCoordinator(0))) {
				// in a case this node is the third node after adding the new
				// node
				// so just deleting from rep1 is enough
				// do nothing!
			} else {
				// when the excoordinator(1) is the newcoordinator(1)
				// removing the data which belongs to newcoordinator(0),
				// which previously was belonged to coordinator(1) from rep2
				rep2.removeDataInRange(parent.getThisServerInfo()
						.getFirstCoordinatorInfo().getFromIndex(), parent
						.getThisServerInfo().getFirstCoordinatorInfo()
						.getToIndex());
			}

			ECSMessage acknowledgeMessage = new ECSMessage();
			acknowledgeMessage.setActionType(ECSCommand.ACK);
			this.sendECSMessage(acknowledgeMessage);
			logger.info("send RemoveData acknowledgment back to  ECS");

		} else if (msg.getActionType().equals(ECSCommand.SEND_METADATA)) {
			logger.info("updating metadata ...");
			parent.setExCoordinator(0, parent.getThisServerInfo()
					.getFirstCoordinatorInfo());
			parent.setExCoordinator(1, parent.getThisServerInfo()
					.getSecondCoordinatorInfo());
			parent.setMetadata(msg.getMetaData());
			
			//swaping replicas!
			if(parent.getExCoordinator(0).equals(parent.getThisServerInfo().getSecondCoordinatorInfo())){
				rep2.putAll(rep1.getDataInRange(parent.getExCoordinator(0).getFromIndex(), parent.getExCoordinator(0).getToIndex()));
				rep1.removeDataInRange(parent.getExCoordinator(0).getFromIndex(), parent.getExCoordinator(0).getToIndex());
			}
			if(parent.getExCoordinator(1).equals(parent.getThisServerInfo().getFirstCoordinatorInfo())){
				rep1.putAll(rep2.getDataInRange(parent.getExCoordinator(1).getFromIndex(), parent.getExCoordinator(1).getToIndex()));
				rep2.removeDataInRange(parent.getExCoordinator(1).getFromIndex(), parent.getExCoordinator(1).getToIndex());			
				
			}
				//cleaning replicas
			// dangerous for recovering data from failed node
			/*if(! parent.getExCoordinator(0).equals(parent.getThisServerInfo().getFirstCoordinatorInfo()))
				rep1.removeDataInRange(parent.getExCoordinator(0).getFromIndex(), parent.getExCoordinator(0).getToIndex());
			
			if(! parent.getExCoordinator(1).equals(parent.getThisServerInfo().getSecondCoordinatorInfo()))
				rep2.removeDataInRange(parent.getExCoordinator(1).getFromIndex(), parent.getExCoordinator(1).getToIndex());	*/		
		}
	}

	private void handleReplicationRequest(ReplicaMessage msg) {
		logger.debug("status for the replication message is : "
				+ msg.getStatus());
		if (msg.getStatus().equals(KVMessage.StatusType.PUT)) {
			if (msg.getCoordinatorServerInfo().equals(
			// parent.getThisServerInfo().getFirstReplicaInfo())) {
					parent.getThisServerInfo().getFirstCoordinatorInfo())) {
				rep1.put(msg.getKey(), msg.getValue());
				logger.debug("replication in replica 1");
			} else if (msg.getCoordinatorServerInfo().equals(
			// parent.getThisServerInfo().getSecondReplicaInfo())) {
					parent.getThisServerInfo().getSecondCoordinatorInfo())) {
				rep2.put(msg.getKey(), msg.getValue());
				logger.debug("replication in replica 2");
			}
		}
		logger.info("replication message processed ");
	}

	private boolean isIndexInRange(String fromIndex, String toIndex,
			ServerInfo server) {
		// the last node in the ring
		logger.debug(fromIndex);
		logger.debug(toIndex);
		logger.debug("SERVER");
		logger.debug(server.getFromIndex());
		logger.debug(server.getToIndex());

		if (server.getFromIndex().compareTo(server.getToIndex()) > 0) {
			boolean b = ((fromIndex.compareTo(server.getFromIndex()) >= 0 || fromIndex
					.compareTo(server.getToIndex()) <= 0) && (toIndex
					.compareTo(server.getFromIndex()) >= 0 || toIndex
					.compareTo(server.getToIndex()) <= 0));
			logger.debug("answer is  " + b + " for " + server);
			return b;
		}
		// for the other nodes
		else {
			boolean b = (fromIndex.compareTo(server.getFromIndex()) >= 0 && fromIndex
					.compareTo(server.getToIndex()) <= 0)
					&& (toIndex.compareTo(server.getFromIndex()) >= 0 && toIndex
							.compareTo(server.getToIndex()) <= 0);
			;

			logger.debug("answer is " + b + " for " + server);
			return b;
		}

	}

	private void handleRecoverDataRequest(RecoverMessage message) {
		logger.debug("INSIDE RECOVER DATA");
		
		if (message.getActionType().equals(ECSCommand.RECOVER_DATA)) {
			logger.debug("INSIDE RECOVER DATA");
			if (message.getFailedServer().equals(
					parent.getThisServerInfo().getFirstCoordinatorInfo())) {
				try {
					logger.debug("recovering ....");
					logger.debug(rep1.getAll());
					dbManager.putAll(rep1.getAll());
					rep1.removeDataInRange(parent.getThisServerInfo().getFirstCoordinatorInfo().getFromIndex(), parent.getThisServerInfo().getFirstCoordinatorInfo().getToIndex());
				} catch (ClassNotFoundException | IOException e) {
					logger.error("Error in retrieving the recovery data from the first replication database");
				}
			} else if (message.getFailedServer().equals(
					parent.getThisServerInfo().getSecondCoordinatorInfo())) {
				try {
					logger.debug("recovering ....");
					logger.debug(rep2.getAll());
					dbManager.putAll(rep2.getAll());
					rep2.removeDataInRange(parent.getThisServerInfo().getFirstCoordinatorInfo().getFromIndex(), parent.getThisServerInfo().getFirstCoordinatorInfo().getToIndex());
				} catch (ClassNotFoundException | IOException e) {
					logger.error("Error in retrieving the recovery data from the second replication database");
				}
			}
			logger.info("data recovered into main database ");
		}
	}

}
