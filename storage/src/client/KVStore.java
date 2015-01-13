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
import java.util.List;

import org.apache.log4j.Logger;

import common.Hasher;
import common.ServerInfo;
import common.messages.AbstractMessage;
import common.messages.AbstractMessage.MessageType;
import common.messages.KVMessage;
import common.messages.ClientMessage;
import common.messages.KVMessage.StatusType;
import common.messages.SubscribeMessage;

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
	}

	public KVStore(ServerInfo serverInfo) {
		this.currentDestinationServer = serverInfo;
		this.metadata = new ArrayList<ServerInfo>();
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
		this.metadata = metadata;
		logger.info("update metadata with " + metadata.size() + " keys");
		logger.debug("meta data received ");
		/*
		 * for (ServerInfo s : metadata) logger.debug(s.getFromIndex() + " " +
		 * s.getToIndex());
		 */
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

	public KVMessage gets(String key,ClientInfo clientInfo) {
		SubscribeMessage msg = new SubscribeMessage();
		KVMessage receivedMsg = null;
		msg.setKey(key);
		msg.setSubscriber(clientInfo);
		msg.setStatusType(StatusType.GET);
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

}
