package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;
import org.apache.log4j.net.SocketServer;

import app_kvEcs.ECSConnectionThread;
import app_kvEcs.ECSImpl;
import utilities.LoggingManager;
import client.ClientInfo;
import client.KVStore;
import common.messages.KVMessage;
import common.messages.ClientMessage;

public class KVClient {

	private KVStore connection; // reference to connection interface
	private Logger logger;
	boolean running;
	ServerSocket serverSocket;

	public KVClient() {
		logger = LoggingManager.getInstance().createLogger(this.getClass());
	}

	/**
	 * The main method that starts the application and interacts with the user
	 * within the defined protocol.
	 */
	public void startApplication() {
		// initialize buffer reader to read user input.
		BufferedReader cons = new BufferedReader(new InputStreamReader(
				System.in));
		logger.debug("Input Stream Reader created");
		// the flag to stop shell interaction
		boolean quit = false;
		while (!quit) {
			System.out.print(UserFacingMessages.ECHO_PROMPT);
			String input;
			String[] tokens;
			try {
				input = cons.readLine();
				tokens = input.trim().split(UserFacingMessages.SPLIT_ON);
				// user input was split as tokens.
				// safety check
				if (tokens == null || tokens.length == 0) {
					throw new IllegalArgumentException(
							UserFacingMessages.GENERAL_ILLIGAL_ARGUMENT);
				}

				// start parsing the tokens
				KVCommand command = KVCommand.fromString(tokens[0]);
				ValidationUtil validationUtil = ValidationUtil.getInstance();
				switch (command) {
				case CONNECT:
					if (validationUtil.isValidConnectionParams(tokens)) {

						connection = new KVStore(tokens[1],
								Integer.parseInt(tokens[2]));

						connection.connect();
						System.out.println("Connected to KV server, "
								+ tokens[1] + ":" + tokens[2]);
						logger.info("Connected to KV server, " + tokens[1]
								+ ":" + tokens[2]);
					}
					break;
				case DISCONNECT:
					connection.disconnect();
					System.out.println("Connection closed.");
					logger.info("Connection closed.");
					break;
				case PUT:
					if (validationUtil.isValidStoreParams(tokens)) {
						KVMessage result = connection.put(tokens[1], tokens[2]);
						String textResult = handleResponse(result, 1);
						logger.info(textResult);
					}
					break;

				case GET:
					if (tokens[1] != null && !tokens[1].isEmpty()) {
						KVMessage result = connection.get(tokens[1]);
						String textResult = handleResponse(result, -1);
						logger.info(textResult);
					} else {
						logger.warn("Key was not provided.");
						System.out.println("Key was not provided.");
					}
					break;
				// TODO: @Arash add proper puts and gets
				case GETS:
					// /start listening socket server for notifications
					if (tokens[1] != null && !tokens[1].isEmpty()) {
						KVMessage result = connection.gets(tokens[1],
								getThisClientInfo());
						String textResult = handleResponse(result, -1);
						logger.info(textResult);
					} else {
						logger.warn("Key was not provided.");
						System.out.println("Key was not provided.");
					}

					break;
				case PUTS:

					break;
				case LOG_LEVEL:
					if (validationUtil.isValidLogLevel(tokens)) {
						LoggingManager.getInstance().setLoggerLevel(tokens[1]);
						logger.info("Log Level Set to: " + tokens[1]);
						System.out.println("Log Level Set to: " + tokens[1]);
					}
					break;
				case HELP:
					System.out.println(UserFacingMessages.HELP_TEXT);
					logger.info("Help Text provided to user.");
					break;

				case UN_SUPPORTED:
					System.out.println(UserFacingMessages.UN_SUPPORTED_COMMAND);
					logger.warn("User entered unsupported command.");
					break;

				case QUIT:

					// /if we are waiting for notification stop waiting
					running = false;
					if (this.listeningThread != null) {
						this.listeningThread.stop();
					}
					quit = true;
					connection.disconnect();
					System.out.println("Quit program based on user request.");
					logger.info("Quit program based on user request.");
					break;

				default:
					break;
				}

			} catch (Exception e) {
				e.printStackTrace();
				logger.error(e.getMessage());
				System.out
						.println("Server is not found any more, make sure the server is up");
			}

		}

	}

	private ClientInfo ThisClientInfo;

	private ClientInfo getThisClientInfo() {
		if (this.ThisClientInfo == null) {
			startListeningForNotifications();
			int listeningPort = serverSocket.getLocalPort();
			String listeningAddress = serverSocket.getInetAddress()
					.getHostAddress();
			this.ThisClientInfo = new ClientInfo();
			this.ThisClientInfo.setAddress(listeningAddress);
			this.ThisClientInfo.setPort(listeningPort);
		}
		return ThisClientInfo;
	}

	/**
	 * 
	 * @param result
	 * @param mode
	 *            : 0 the default, 1 when handleResponse is called from put
	 *            method, -1 called from get method
	 * @return
	 * @throws IOException
	 */
	private String handleResponse(KVMessage result, int mode)
			throws IOException {
		String resultText = "";
		switch (result.getStatus()) {
		case GET_ERROR:
			resultText = UserFacingMessages.GET_ERROR_MESSAGE
					+ result.getValue();
			break;
		case GET_SUCCESS:
			resultText = UserFacingMessages.GET_SUCCESS_MESSAGE
					+ result.getValue();
			break;

		case PUT_ERROR:
			resultText = UserFacingMessages.PUT_ERROR_MESSAGE
					+ result.getValue();
			break;

		case PUT_SUCCESS:
			resultText = UserFacingMessages.PUT_SUCCESS_MESSAGE;
			break;

		case PUT_UPDATE:
			resultText = UserFacingMessages.PUT_UPDATE_MESSAGE;
			break;

		case DELETE_SUCCESS:
			resultText = UserFacingMessages.DELETE_SUCCESS_MESSAGE;
			break;
		case DELETE_ERROR:
			resultText = UserFacingMessages.DELETE_ERROR_MESSAGE
					+ result.getValue();
			break;
		case SERVER_NOT_RESPONSIBLE: {
			// resultText = UserFacingMessages.SERVER_NOT_RESPONSIBLE;
			this.connection.updateMetadata(((ClientMessage) result)
					.getMetadata());
			// the previous command was put because mode is 1
			if (mode == 1) {
				// if clause added to avoid infinite loop when a server is
				// faulty
				if (connection.getCurrentConnection().equals(
						connection.getDestinationServerInfo(result.getKey()))) {
					resultText = "SERVER IS NOT RESPONSIBLE RECIEVED"
							+ " FROM THE RESPONSIBLE SERVER! PLEASE CHANGE CONNECTION AND TRY AGAIN!";
					logger.debug(" Current connection "
							+ connection.getCurrentConnection().getFromIndex()
							+ " "
							+ connection.getCurrentConnection().getToIndex()
							+ " respinsible form latest metaData "
							+ connection.getDestinationServerInfo(
									result.getKey()).getFromIndex()
							+ " "
							+ connection.getDestinationServerInfo(
									result.getKey()).getToIndex());
					return resultText;

				}

				logger.info("SERVER NOT RESPONSIBLE.\n Changing Connection...");
				// tell the connection to switch the connection to the
				// responsible server
				this.connection.switchConnection(connection
						.getDestinationServerInfo(result.getKey()));
				result = this.connection
						.put(result.getKey(), result.getValue());
				resultText = handleResponse(result, 1);
			}

			// the previous command was get because mode is -1
			else if (mode == -1) {
				// if clause added to avoid infinite loop when a server is
				// faulty
				if (connection.getCurrentConnection().equals(
						connection.getDestinationServerInfo(result.getKey()))) {
					resultText = "SERVER IS NOT RESPONSIBLE RECIEVED"
							+ " FROM THE RESPONSIBLE SERVER! PLEASE CHANGE CONNECTION AND TRY AGAIN!";
					return resultText;
				}
				logger.info("SERVER NOT RESPONSIBLE.\n Changing Connection...");
				// tell the connection to switch the connection to the
				// responsible server
				this.connection.switchConnection(connection
						.getDestinationServerInfo(result.getKey()));
				result = this.connection.get(result.getKey());
				resultText = handleResponse(result, -1);
			}
			break;

		}

		case SERVER_STOPPED: {
			resultText = UserFacingMessages.SERVER_STOPPED;
			break;
		}

		case SERVER_WRITE_LOCK: {
			resultText = UserFacingMessages.SERVER_WRITE_LOCK;
			break;
		}

		default:
			resultText = result.getStatus() + result.getKey()
					+ result.getValue();
		}

		return resultText;
	}

	Thread listeningThread;

	// /TODO: @Arash call this when you want to start listening to the
	// notifications from server
	private void startListeningForNotifications() {
		initializeServer();
		running = true;
		final KVClient that = this;
		listeningThread = new Thread(new Runnable() {
			@Override
			public void run() {

				if (serverSocket != null) {
					while (running) {
						try {
							logger.debug("NOTIFICATION: waiting for notifications");
							Socket client = serverSocket.accept();
							ClientNotificationThread connection = new ClientNotificationThread(
									client, that);
							new Thread(connection).start();

							logger.info("NOTIFICATION: new Connection: Connected to "
									+ client.getInetAddress().getHostName()
									+ " on port " + client.getPort());
						} catch (IOException e) {
							logger.error("NOTIFICATION: Error! "
									+ "Unable to establish connection. \n", e);
						}
					}
				}
				logger.info("NOTIFICATION Server stopped.");
			}
		});
		listeningThread.start();
	}

	private synchronized boolean initializeServer() {
		logger.info("NOTIFICATION socket server is being initialized...");
		try {
			// /port number 0 let java decide and pick the free port for us
			serverSocket = new ServerSocket(0);
			logger.info("Server listening on: " + serverSocket.getLocalPort()
					+ serverSocket.getInetAddress());

			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if (e instanceof BindException) {
				logger.error("Port " + serverSocket.getLocalPort()
						+ " is already bound!");
			}
			return false;
		}
	}

	public void reportNotification(String key, String value) {
		// TODO: update cache @Arash
		logger.debug("NOTIFICATION recieved for key:" + key + ", value:"
				+ value);
	}
}