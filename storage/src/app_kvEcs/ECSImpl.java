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
import client.SerializationUtil;
import utilities.LoggingManager;
import common.Hasher;
import common.ServerInfo;

/**
 * implementation of the ECS.java, responsible for main operations of the ECS
 * 
 *
 */

public class ECSImpl implements ECS {

	private List<ServerInfo> serverRepository;
	/**
	 * The meta data
	 */
	private List<ServerInfo> activeServers;
	private Map<ServerInfo, ServerConnection> activeConnections;
	Logger logger = LoggingManager.getInstance().createLogger(this.getClass());
	private Hasher md5Hasher;
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

		// some nodes were not started successfully, try to incrementally add
		// the remaining servers!
		if (serversToStart.size() < numberOfNodes) {
			int n = numberOfNodes - serversToStart.size();
			count = 0;
			int i = 0, r;
			while (count < n && i < (serverRepository.size() - 1)) {
				temp = serverRepository.get(i);
				if ((!serversToStart.contains(temp))
						&& !this.activeServers.contains(temp)) {
					r = launchNode(temp);
					if (r == 0) {
						// server started successfully
						serversToStart.add(temp);
						temp.setServerLaunched(true);
						count++;
					}
				}
				i++;
			}
			// when ECS is not successful to start all the n servers requested
			// by the user
			if (count < n)
				logger.warn("Could not start all the " + numberOfNodes
						+ " servers! insetead started "
						+ this.activeServers.size() + " servers");
		}
		final ECSImpl that = this;
		// start listening to failure detection reports
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
		serversToStart = calculateMetaData(activeServers);

		// communicate with servers and send call initialize command
		ECSMessage initMessage = getInitMessage(serversToStart);
		logger.info("Sending init signals to servers");

		// create server connection for further communication with the servers
		for (ServerInfo server : this.activeServers) {
			ServerConnection channel = new ServerConnection(server);
			try {
				channel.connect();
				channel.sendMessage(initMessage);
				activeConnections.put(server, channel);
				channel.disconnect();
			} catch (IOException e) {
				logger.error("One server node couldn't be initiated" + server);
			}
		}
		logger.info("Active servers are launched and handed meta-data.");

		logger.debug("Staring Replication Operation...");
		replicationOperation();
		logger.info(" Replication operation is done");
		logger.info("ECS started " + serversToStart.toString());
	}

	/**
	 * start the ECS Server, which is responsible for getting failure detection
	 * reports from the KVServers
	 * 
	 * @return true if the ecs server started successfully
	 */
	private boolean initializeServer() {
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
	 * tell each KVServer to replicated its data on the next two
	 * nodes(KVServer's replicas) called during initialization of the system
	 * 
	 * @param serversToStart
	 * @return void
	 */
	private void replicationOperation() {
		List<ServerConnection> locked = new ArrayList<ServerConnection>();
		for (ServerInfo serverNode : activeServers) {
			List<ServerInfo> replicas = getReplicas(serverNode);
			sendData(serverNode, replicas.get(0), serverNode.getFromIndex(),
					serverNode.getToIndex());
			sendData(serverNode, replicas.get(1), serverNode.getFromIndex(),
					serverNode.getToIndex());
			locked.add(activeConnections.get(serverNode));
		}
		// releasing locks, which were put on the servers, after sending
		// sendData command.
		ECSMessage releaseLock = new ECSMessage();
		releaseLock.setActionType(ECSCommand.RELEASE_LOCK);
		try {
			for (ServerConnection sChannel : locked)
				sendECSCommand(sChannel, releaseLock);
			logger.debug("All locks are released.");

			// put back the KVservers statue as under initialization
			ECSMessage underInit = new ECSMessage();
			underInit.setActionType(ECSCommand.INIT);
			underInit.setMetaData(activeServers);
			for (ServerConnection sChannel : locked) {
				sendECSCommand(sChannel, underInit);
			}
			logger.debug("all servers have been put under init");
		} catch (IOException e) {
			logger.error("ReLease Lock message couldn't be sent.");
		}
	}

	/**
	 * makes the initialization message to be sent to the server after starting
	 * them up
	 * 
	 * @param serversToStart
	 * @return ECSMessage INIT
	 */
	private ECSMessage getInitMessage(List<ServerInfo> serversToStart) {
		ECSMessage initMessage = new ECSMessage();
		initMessage.setActionType(ECSCommand.INIT);
		initMessage.setMetaData(serversToStart);
		return initMessage;
	}

	/**
	 * try to launch the servers in the list with remote SSH Calls
	 * 
	 * @param serversToStart
	 */
	private void launchNodes(List<ServerInfo> serversToStart) {
		/*
		 * it is considered that the invoker and invoked processes are in the
		 * same folder and machine
		 */
		String path = System.getProperty("user.dir");
		String command = "nohup java -jar " + path + "/ms3-server.jar ";
		String arguments[] = new String[2];
		arguments[1] = "  ERROR &";
		int result;

		Iterator<ServerInfo> iterator = serversToStart.iterator();
		while (iterator.hasNext()) {
			ServerInfo item = iterator.next();
			arguments[0] = String.valueOf(item.getPort());

			// for ssh calls
			if (!localCall)
				result = processInvoker.invokeProcess(item.getAddress(),
						command, arguments);

			// for local calls (for local testing, calls are made without SSH)
			else
				result = processInvoker.localInvokeProcess(command, arguments);

			// the server started successfully
			if (result == 0) {
				this.activeServers.add(item);
				item.setServerLaunched(true);
			}

			// could not start the server
			else
				iterator.remove();
		}
	}

	/**
	 * launch a single server
	 * 
	 * @param serverToStart
	 * @return 0 in case of successful launch
	 */
	private int launchNode(ServerInfo serverToStart) {
		/*
		 * it is considered that the invoker and invoked processes are in the
		 * same folder and machine
		 */
		String path = System.getProperty("user.dir");
		String command = "nohup java -jar " + path + "/ms3-server.jar ";
		String arguments[] = new String[2];
		arguments[1] = "  ERROR &";
		int result;
		arguments[0] = String.valueOf(serverToStart.getPort());

		// for ssh calls
		if (!localCall)
			result = processInvoker.invokeProcess(serverToStart.getAddress(),
					command, arguments);

		// for local invocations
		else
			result = processInvoker.localInvokeProcess(command, arguments);

		// remote server started successfully
		if (result == 0) {
			this.activeServers.add(serverToStart);
			serverToStart.setServerLaunched(true);
			return 0;
		} else
			return -1;
	}

	/**
	 * calculates the metaData of the current system
	 * 
	 * @param serversToStart
	 *            , servers of the current state of the system
	 * @return the new metaData
	 */
	private List<ServerInfo> calculateMetaData(List<ServerInfo> serversToStart) {

		// calculate each server's MD5 Hash value and sort them based on this
		// value
		for (ServerInfo server : serversToStart) {
			String hashKey = md5Hasher.getHash(server.getAddress() + ":"
					+ server.getPort());
			server.setToIndex(hashKey);
		}
		Collections.sort(serversToStart, new Comparator<ServerInfo>() {
			@Override
			public int compare(ServerInfo o1, ServerInfo o2) {
				return md5Hasher.compareHashes(o1.getToIndex(), o2.getToIndex());
			}
		});

		// setting predecessor,coordinators and replicas for each server
		for (int i = 0; i < serversToStart.size(); i++) {
			ServerInfo server = serversToStart.get(i);
			ServerInfo predecessor;
			if (i == 0) {
				// first node is a special case.
				predecessor = serversToStart.get(serversToStart.size() - 1);
			} else {
				predecessor = serversToStart.get(i - 1);
			}
			server.setFromIndex(predecessor.getToIndex());
		}
		for (ServerInfo s : serversToStart) {
			s.setFirstCoordinatorInfo(getMasters(s).get(0));
			s.setSecondCoordinatorInfo(getMasters(s).get(1));
			s.setFirstReplicaInfo(getReplicas(s).get(0));
			s.setSecondReplicaInfo(getReplicas(s).get(1));
		}
		return serversToStart;
	}

	@Override
	public void start() {
		ECSMessage startMessage = new ECSMessage();
		startMessage.setActionType(ECSCommand.START);
		for (ServerInfo server : this.activeServers) {
			try {
				ServerConnection channel = activeConnections.get(server);
				channel.connect();
				channel.sendMessage(startMessage);
				channel.disconnect();
			} catch (IOException e) {
				logger.error("Could not send message to server" + server
						+ e.getMessage());
			}
		}
		logger.info("Active servers are started.");
	}

	@Override
	public void shutdown() {
		ECSMessage shutDownMessage = new ECSMessage();
		shutDownMessage.setActionType(ECSCommand.SHUT_DOWN);
		for (ServerInfo server : this.activeServers) {
			try {
				ServerConnection channel = activeConnections.get(server);
				channel.connect();
				channel.sendMessage(shutDownMessage);
				channel.disconnect();
			} catch (IOException e) {
				logger.error("Could not send message to server" + server
						+ e.getMessage());
			}
		}
		this.activeConnections.clear();
		this.activeServers.clear();
		logger.info("Active servers are shutdown.");
	}

	@Override
	public void stop() {
		ECSMessage stopMessage = new ECSMessage();
		stopMessage.setActionType(ECSCommand.STOP);

		for (ServerInfo server : this.activeServers) {
			try {
				ServerConnection channel = activeConnections.get(server);
				channel.connect();
				channel.sendMessage(stopMessage);
				channel.disconnect();
			} catch (IOException e) {
				logger.error("Could not send message to server" + server
						+ e.getMessage());
			}
		}
		logger.info("Active servers are stopped.");
	}

	/**
	 * chooses a node randomly from the serverRepository and add it to the
	 * system
	 */
	@Override
	public void addNode() {
		logger.debug("$$$$$$ SYSTEM BEFORE ADDING A NODE $$$$$$");
		for (ServerInfo s : activeServers)
			logger.debug(s.getPort() + s.getToIndex());
		logger.debug("$$$$$$");

		int result = -1;
		int i = 0;
		ServerInfo newNode = new ServerInfo();

		// search in serverRepository to find the servers which are
		// not active (not launched yet), and try to launch them
		while (i < (serverRepository.size() - 1)) {
			newNode = serverRepository.get(i);
			if (!this.activeServers.contains(newNode)) {
				result = launchNode(newNode);
				if (result == 0) {
					// server started successfully
					break;
				}
			}
			i++;
		}
		if (newNode == null) {
			logger.info("No available node to add.");
			return;
		} else if (result != 0) {
			logger.warn("Could not add a new Server!");
			return;
		}

		calculateMetaData(activeServers);

		// 1.initialize the newNode.
		ECSMessage initMessage = getInitMessage(this.activeServers);
		ServerConnection channel = new ServerConnection(newNode);
		try {
			channel.connect();
			channel.sendMessage(initMessage);
			activeConnections.put(newNode, channel);
			logger.info("The new node added" + newNode.getAddress() + ":"
					+ newNode.getPort());
		} catch (IOException e) {

			// server could not be initiated so remove it from the list!
			logger.error(" server node couldn't be initiated"
					+ newNode.getAddress() + ":" + newNode.getPort()
					+ " Operation addNode Not Successfull");
			activeServers.remove(newNode);
			channel.disconnect();
			activeConnections.remove(channel);
			calculateMetaData(activeServers);
			return;
		}

		// 2. start the newNode
		ECSMessage startMessage = new ECSMessage();
		startMessage.setActionType(ECSCommand.START);
		try {
			channel.sendMessage(startMessage);
		} catch (IOException e) {

			// server could not be started so remove it from the list!
			logger.error("Start message couldn't be sent to "
					+ newNode.getAddress() + ":" + newNode.getPort()
					+ " Operation addNode Not Successfull");
			activeServers.remove(newNode);
			channel.disconnect();
			activeConnections.remove(newNode);
			calculateMetaData(activeServers);
			return;
		}

		logger.debug("$$$$$$ NEW SYSTEM STATE $$$$$");
		for (ServerInfo s : activeServers)
			logger.debug(s.getAddress() + ":" + s.getPort() + s.getToIndex());
		logger.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");

		logger.debug("<<<<< Getting data from the successor >>>>>");
		ServerInfo successor = getSuccessor(newNode);
		List<ServerInfo> masters = getMasters(newNode);
		List<ServerInfo> replicas = getReplicas(newNode);

		/*
		 * locked is a list to keep track of the servers which has been put into
		 * the locked mode because of sendData operations
		 */
		List<ServerConnection> locked = new ArrayList<ServerConnection>();

		// handling special case where there is just one node in the system
		if (activeServers.size() <= 2) {

			/*
			 * 3. tell the successor to send the data, which it does not own
			 * anymore to the newNode, tell the two new Coordinators to send
			 * their data for replication to the newNode
			 */
			if (sendData(successor, newNode, newNode.getFromIndex(),
					newNode.getToIndex()) == 0) {
				locked.add(activeConnections.get(successor));
				logger.debug("move data was successfull to the new Node");
				sendMetaData();

				// 4. replicate new Node's data on its replicas
				if (sendData(newNode, successor, newNode.getFromIndex(),
						newNode.getToIndex()) == 1
						|| sendData(successor, newNode,
								successor.getFromIndex(),
								successor.getToIndex()) == 1)

					logger.warn("One or two of the replication operation for node "
							+ newNode.getAddress()
							+ ":"
							+ newNode.getPort()
							+ "failed");
				else {
					logger.debug("move data was sent to new Node to move"
							+ " its replication to two new replica : "
							+ successor.getPort());

					logger.debug("data from " + successor.getAddress() + ":"
							+ successor.getPort() + " is replicated in : "
							+ newNode.getAddress() + ":" + newNode.getPort());

					locked.add(activeConnections.get(newNode));

					// release the lock from the successor, new Node
					logger.debug("releaing the locks");
					ECSMessage releaseLock = new ECSMessage();
					releaseLock.setActionType(ECSCommand.RELEASE_LOCK);
					try {
						for (ServerConnection sChannel : locked) {
							sendECSCommand(sChannel, releaseLock);
						}
						logger.debug("All locks are released.");
					} catch (IOException e) {
						logger.error("ReLease Lock message couldn't be sent.");
					}
					channel.disconnect();
					return;
				}
			} else {
				/*
				 * when move data from successor to the newNode was not
				 * successful data could not be moved to the newly added Server
				 */
				logger.error("Could not move data from "
						+ successor.getServerName() + " to "
						+ newNode.getServerName());
				logger.error("Operation addNode Not Successfull");
				activeServers.remove(newNode);
				channel.disconnect();
				activeConnections.remove(newNode);
				calculateMetaData(activeServers);
				return;
			}
		}

		/*
		 * handling normal cases when there is already more than two nodes in
		 * the system (before adding the new node)
		 */
		else {

			/*
			 * 3. tell the successor to send the data, which it does not own
			 * anymore to the newNode, tell the two new Coordinators to send
			 * their data for replication to the newNode
			 */
			if (sendData(successor, newNode, newNode.getFromIndex(),
					newNode.getToIndex()) == 0
					&& sendData(masters.get(0), newNode, masters.get(0)
							.getFromIndex(), masters.get(0).getToIndex()) == 0
					&& sendData(masters.get(1), newNode, masters.get(1)
							.getFromIndex(), masters.get(1).getToIndex()) == 0) {

				locked.add(activeConnections.get(successor));
				locked.add(activeConnections.get(masters.get(0)));
				locked.add(activeConnections.get(masters.get(1)));

				logger.debug("move data message was sent to the new masters : "
						+ masters.get(0).getAddress() + ":"
						+ masters.get(0).getPort() + "   "
						+ masters.get(1).getAddress() + ":"
						+ masters.get(1).getPort());

				/*
				 * 4.Sending metaData updates to the new node's replicas and
				 * sending new Node's data for replication to replicas
				 */
				ECSMessage metaDataUpdate = new ECSMessage();
				metaDataUpdate.setActionType(ECSCommand.SEND_METADATA);
				metaDataUpdate.setMetaData(activeServers);
				try {
					ServerConnection replicaChannel = new ServerConnection(
							replicas.get(0));
					sendECSCommand(replicaChannel, metaDataUpdate);
					replicaChannel = new ServerConnection(replicas.get(1));
					sendECSCommand(replicaChannel, metaDataUpdate);
				} catch (IOException e) {
					logger.error("Meta-data update couldn't be sent."
							+ e.getMessage());
				}

				// sending newNode's data for replication to the its replicas
				logger.debug("<<<< sending new Nodes data to the new replicas >>>>");
				if (sendData(newNode, replicas.get(0), newNode.getFromIndex(),
						newNode.getToIndex()) == 1
						|| sendData(newNode, replicas.get(1),
								newNode.getFromIndex(), newNode.getToIndex()) == 1)
					logger.warn("One or two of the replication operation for node "
							+ newNode.getAddress()
							+ ":"
							+ newNode.getPort()
							+ "failed");
				else {
					logger.debug("move data was sent to new Node to move"
							+ " its replication to two new replicas : "
							+ replicas.get(0).getAddress() + ":"
							+ replicas.get(0).getPort() + "   "
							+ replicas.get(1).getAddress() + ":"
							+ replicas.get(1).getPort());
					locked.add(activeConnections.get(newNode));

					/*
					 * 5.tell to the next two nodes (newNodes replicas) and the
					 * third node (which has the old data from new Nodes
					 * replica(0) as replication)to delete their replicated data
					 * which they used to store from newNode's Masters
					 */
					logger.debug("<<<< removing old replicated data from the two next nodes after the "
							+ "new node >>>>>");

					// when we have 3 nodes in system This operation is not
					// needed!
					if (activeServers.size() > 3) {
						int r = removeData(replicas.get(0), masters.get(0)
								.getFromIndex(), masters.get(0).getToIndex());
						r += removeData(replicas.get(1), masters.get(1)
								.getFromIndex(), masters.get(1).getToIndex());
						r += removeData(replicas.get(1).getFirstReplicaInfo(),
								newNode.getFromIndex(), newNode.getToIndex());

						if (r == 0)
							logger.debug("remove data message was sent to two new replicas : "
									+ replicas.get(0).getAddress()
									+ ":"
									+ replicas.get(0).getPort()
									+ "   "
									+ replicas.get(1).getAddress()
									+ ":"
									+ replicas.get(1).getPort());
						else
							logger.warn("remove data message could not be sent to two new replicas : "
									+ replicas.get(0).getAddress()
									+ ":"
									+ replicas.get(0).getPort()
									+ "   "
									+ replicas.get(1).getAddress()
									+ ":"
									+ replicas.get(1).getPort()
									+ " "
									+ replicas.get(1).getFirstReplicaInfo());

					}
				}

				// 6.sends MetaData to all the servers!
				sendMetaData();
				channel.disconnect();

				logger.debug("releaing the locks");
				// 7. release the lock from the successor, new Node and masters
				ECSMessage releaseLock = new ECSMessage();
				releaseLock.setActionType(ECSCommand.RELEASE_LOCK);
				try {
					for (ServerConnection sChannel : locked) {
						sendECSCommand(sChannel, releaseLock);
					}
					logger.debug("All locks are released.");
				} catch (IOException e) {
					logger.error("ReLease Lock message couldn't be sent.");
				}
			}

			/*
			 * when move data from successor and newNode's Coordinators to the
			 * newNode was not successful
			 */
			else {
				// data could not be moved to the newly added Server
				logger.error("Could not move data from "
						+ successor.getServerName() + " to "
						+ newNode.getServerName());
				logger.error("Operation addNode Not Successfull");
				activeServers.remove(newNode);
				channel.disconnect();
				activeConnections.remove(newNode);
				calculateMetaData(activeServers);
				return;
			}
		}
	}

	/**
	 * returns the successor of the newNode
	 * 
	 * @param newNode
	 * @return
	 */
	private ServerInfo getSuccessor(ServerInfo newNode) {
		ServerInfo successor;
		int nodeIndex = this.activeServers.indexOf(newNode);
		try {
			successor = this.activeServers.get(nodeIndex + 1);
		} catch (IndexOutOfBoundsException e) {
			/*
			 * that means the new node is the last item and the successor with
			 * be the first item.
			 */
			successor = this.activeServers.get(0);
		}

		return successor;
	}

	/**
	 * gets a serverInfo and return two other Server info which are responsible
	 * for keeping the replicated data of the server s
	 * 
	 * @param s
	 * @return List of ServerInfo, which are responsible for keeping replicated
	 *         data of the s
	 */
	private List<ServerInfo> getReplicas(ServerInfo s) {
		if (activeServers.contains(s)) {
			int i, r1, r2, l;
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
		} else
			return null;
	}

	/**
	 * returns the two server (Coordinators),which the server s is keeping their
	 * replicated data
	 * 
	 * @param s
	 * @return List of ServerInfo of the Coordinators(Masters)
	 */
	private List<ServerInfo> getMasters(ServerInfo s) {
		if (activeServers.contains(s)) {
			int i, m1, m2, l;
			i = activeServers.indexOf(s);
			l = activeServers.size();
			m1 = i - 1;
			m2 = i - 2;
			if (m1 < 0)
				m1 += l;
			if (m2 < 0)
				m2 += l;
			// specail case when the size of active servers is one!
			if (m2 < 0)
				m2 += 1;

			ArrayList<ServerInfo> masters = new ArrayList<ServerInfo>();
			// first add the master which is far from the node
			masters.add(activeServers.get(m2));
			masters.add(activeServers.get(m1));
			return masters;
		} else
			return null;
	}

	/**
	 * removes a node randomly from the active servers!
	 */
	@Override
	public boolean removeNode() {
		int removeIndex = pickRandomValue(this.activeServers.size(), true);
		logger.debug("Picked node index to remove " + removeIndex);
		ServerInfo nodeToDelete = this.activeServers.get(removeIndex);
		ServerInfo successor = getSuccessor(nodeToDelete);
		if (nodeToDelete.equals(successor)) {
			logger.error("Cannot remove node because it is the only active node available, If you want to remove please use the shutdown option");
			return false;
		} else {
			return removeNode(nodeToDelete);
		}
	}

	/**
	 * remove the nodeToDelete node from the system
	 * 
	 * @param failed
	 * @return
	 */
	private boolean removeNode(ServerInfo nodeToDelete) {
		ServerInfo successor = getSuccessor(nodeToDelete);
		List<ServerInfo> masters = getMasters(nodeToDelete);
		List<ServerInfo> replicas = getReplicas(nodeToDelete);
		ServerConnection nodeToDeleteChannel = this.activeConnections
				.get(nodeToDelete);
		ServerConnection successorChannel = this.activeConnections
				.get(successor);
		/*
		 * A list to keep track of the serves, which have been put under locked
		 * statue on the result of some operation such as sendData
		 */
		List<ServerConnection> locked = new ArrayList<ServerConnection>();
		this.activeServers.remove(nodeToDelete);
		calculateMetaData(this.activeServers);

		logger.debug("$$$$$$ SYSTEM BEFORE REMOVING $$$$$$");
		for (ServerInfo s : activeServers)
			logger.debug(s.getAddress() + ":" + s.getPort());
		logger.debug("$$$$$$");

		/*
		 * special cases when we would have just one or two nodes left after
		 * removal
		 */
		if (activeServers.size() <= 2) {
			logger.debug("<<<< invoking transfers of data >>>>");
			/*
			 * 1. sendingmetaData to all is same as to sending metaData to
			 * replicas because just two node is left
			 */
			sendMetaData();
			// 2. Trasfer of the data
			int result = sendData(nodeToDelete, successor,
					nodeToDelete.getFromIndex(), nodeToDelete.getToIndex());

			/*
			 * 3. telling to the remaining Nodes to replicate their data on each
			 * other
			 */
			if (activeServers.size() == 2) {
				result += sendData(activeServers.get(0), activeServers.get(1),
						activeServers.get(0).getFromIndex(),
						activeServers.get(0).getToIndex());
				result += sendData(activeServers.get(1), activeServers.get(0),
						activeServers.get(1).getFromIndex(),
						activeServers.get(1).getToIndex());
				locked.add(activeConnections.get(activeServers.get(0)));
				locked.add(activeConnections.get(activeServers.get(1)));

				logger.debug("Data for replication sent from "
						+ activeServers.get(0).getAddress() + ":"
						+ activeServers.get(0).getPort() + " to "
						+ activeServers.get(1).getAddress() + ":"
						+ activeServers.get(1).getPort());
				logger.debug("Data sent for replication from "
						+ activeServers.get(1).getAddress() + ":"
						+ activeServers.get(1).getPort() + " to "
						+ activeServers.get(0).getAddress() + ":"
						+ activeServers.get(0).getPort());
			}
			if (result == 0) {
				// everything went perfect
				logger.debug("Data sent from " + nodeToDelete.getAddress()
						+ ":" + nodeToDelete.getPort() + " to "
						+ successor.getAddress() + ":" + successor.getPort());
			} else {
				/*
				 * move data from nodeToDelet failed getting back the system to
				 * the previous stage:(before removing)
				 */
				logger.error("SendData Unsuccessful! Delete Node Operation failed");
				logger.error("Getting back the system to the previous state ");
				try {
					this.activeServers.add(nodeToDelete);
					this.activeConnections.put(nodeToDelete,
							new ServerConnection(nodeToDelete));
					ECSMessage releaseLock = new ECSMessage();
					releaseLock.setActionType(ECSCommand.RELEASE_LOCK);
					sendECSCommand(nodeToDeleteChannel, releaseLock);
					sendECSCommand(new ServerConnection(masters.get(0)),
							releaseLock);
					sendECSCommand(new ServerConnection(masters.get(1)),
							releaseLock);
					calculateMetaData(activeServers);
					sendMetaData();
					return false;
				} catch (IOException e) {
					logger.error("Write lock message couldn't be sent."
							+ e.getMessage());
				}
			}
		}

		/* in normal cases when we have more than 2 nodes left after removal */
		else {
			/*
			 * 1. Send meta-data update to the successor node and the replica(1)
			 * for nodeTODelete we dont send metaData update to replica(0)
			 * because replica(0) is same as successor! we also have to send
			 * metadata update to replica(1) of the successor
			 */
			ECSMessage metaDataUpdate = new ECSMessage();
			metaDataUpdate.setActionType(ECSCommand.SEND_METADATA);
			metaDataUpdate.setMetaData(activeServers);
			ServerConnection replicaChannel = new ServerConnection(
					replicas.get(1));
			try {
				sendECSCommand(successorChannel, metaDataUpdate);
				sendECSCommand(replicaChannel, metaDataUpdate);
				sendECSCommand(
						new ServerConnection(successor.getSecondReplicaInfo()),
						metaDataUpdate);
			} catch (IOException e) {
				logger.error("Meta-data update couldn't be sent."
						+ e.getMessage());
			}

			logger.debug("<<<< invoking transfers of data >>>>");

			// 2. Invoke the transfer of the affected data items

			if (sendData(nodeToDelete, successor, nodeToDelete.getFromIndex(),
					nodeToDelete.getToIndex()) == 0) {
				logger.debug("Data sent from " + nodeToDelete.getPort()
						+ " to " + successor.getPort());

				/*
				 * 3. Tell the two masters to send their data as replicas to
				 * their two new replicas invoking the transfer of nodetoDlete's
				 * data to the successors replicas
				 */
				if (sendData(masters.get(0), replicas.get(0), masters.get(0)
						.getFromIndex(), masters.get(0).getToIndex()) == 0) {
					locked.add(activeConnections.get(masters.get(0)));
					logger.debug("Data for replication sent from "
							+ masters.get(0).getAddress() + ":"
							+ masters.get(0).getPort() + " to "
							+ replicas.get(0).getAddress() + ":"
							+ replicas.get(0).getPort());
				}
				if (sendData(masters.get(1), replicas.get(1), masters.get(1)
						.getFromIndex(), masters.get(1).getToIndex()) == 0) {
					locked.add(activeConnections.get(masters.get(1)));
					logger.debug("Data for replication sent from "
							+ masters.get(1).getAddress() + ":"
							+ masters.get(1).getPort() + " to "
							+ replicas.get(1).getAddress() + ":"
							+ replicas.get(1).getPort());

				}
				/*
				 * 4. invoking replication of the new data moved to successor:
				 * we used from and to index from the successor so successor
				 * will treat it as replication not move data to another server
				 */
				if (sendData(successor, successor.getFirstReplicaInfo(),
						successor.getFromIndex(), successor.getToIndex())
						+ sendData(successor, successor.getSecondReplicaInfo(),
								successor.getFromIndex(),
								successor.getToIndex()) <= 1)
					locked.add(activeConnections.get(successor));

				/*
				 * 5.tell to the next two nodes (newNodes replicas) to delete
				 * their replicated data which they used to store from newNode's
				 * Masters we don't send remove data to the replica(0) because
				 * replica(0) is the successor and owns the data which used to
				 * belong to the node to be removed
				 */
				int r = removeData(replicas.get(1),
						nodeToDelete.getFromIndex(), nodeToDelete.getToIndex());
				if (r == 0)
					logger.debug("remove data message was sent to two new replicas : "
							+ replicas.get(0).getAddress()
							+ ":"
							+ replicas.get(0).getPort()
							+ "   "
							+ replicas.get(1).getAddress()
							+ ":"
							+ replicas.get(1).getPort());

				else
					logger.warn("remove data message could not be sent to two new replicas : "
							+ replicas.get(0).getPort()
							+ "   "
							+ replicas.get(1).getPort());

				// 6. send metadata update to all
				sendMetaData();

			} else {
				/*
				 * move data from nodeToDelet failed getting back the system to
				 * the previous stage:(before removing)
				 */
				logger.error("SendData Unsuccessful! Delete Node Operation failed");
				logger.error("Getting back the system to the previous state ");
				try {
					this.activeServers.add(nodeToDelete);
					ECSMessage releaseLock = new ECSMessage();
					releaseLock.setActionType(ECSCommand.RELEASE_LOCK);
					sendECSCommand(nodeToDeleteChannel, releaseLock);
					sendECSCommand(new ServerConnection(masters.get(0)),
							releaseLock);
					sendECSCommand(new ServerConnection(masters.get(1)),
							releaseLock);
					calculateMetaData(activeServers);
					sendMetaData();
					return false;
				} catch (IOException e) {
					logger.error("Write lock message couldn't be sent."
							+ e.getMessage());
				}
			}
		}

		// 7(4 for special cases). shut down the nodetoDelete
		ECSMessage shutDown = new ECSMessage();
		shutDown.setActionType(ECSCommand.SHUT_DOWN);
		try {
			sendECSCommand(nodeToDeleteChannel, shutDown);
		} catch (IOException e) {
			logger.error("shut down message couldn't be sent." + e.getMessage());
		}

		// 8(5 for special cases). step releasing all locks
		logger.debug("releasing all locks!! ");
		ECSMessage releaseLock = new ECSMessage();
		releaseLock.setActionType(ECSCommand.RELEASE_LOCK);
		try {
			for (ServerConnection sChannel : locked) {
				sendECSCommand(sChannel, releaseLock);
				logger.debug("lock on " + sChannel.getServer().getPort()
						+ " released");
			}
			logger.debug("All locks are released.");
		} catch (IOException e) {
			logger.error("ReLease Lock message couldn't be sent.");
		}

		// 9.(6)
		nodeToDeleteChannel.disconnect();
		cleanUpNode(nodeToDelete);

		logger.debug("$$$$$$ New SYSTEM STATE $$$$$$");
		for (ServerInfo s : activeServers)
			logger.debug(s.getAddress() + ":" + s.getPort());
		logger.debug("$$$$$$");

		return true;
	}

	/**
	 * tries to recover the System when a failure is detected
	 * 
	 * @param failed
	 *            Node
	 */
	private void recovery(ServerInfo failedNode) {
		List<ServerConnection> locked = new ArrayList<ServerConnection>();
		// we have not removed the failedNode from the activeServers yet
		if (activeServers.size() == 1) {
			logger.error("System failed! All nodes are dead! All data is lost!");
			return;
		} else if (activeServers.size() == 2) {
			logger.debug("inside REcovery ! Special case, just one node left in System");
			/*
			 * 1. send a command to store replicated data as the main data on
			 * theremaining server
			 */
			RecoverMessage recoverData = new RecoverMessage();
			recoverData.setActionType(ECSCommand.RECOVER_DATA);
			recoverData.setFailedServer(failedNode);
			try {
				ServerConnection serverChannel = activeConnections
						.get(getSuccessor(failedNode));
				serverChannel.connect();
				serverChannel.sendMessage(recoverData);
				serverChannel.disconnect();

			} catch (IOException e) {
				logger.error("recover Data message couldn't be sent."
						+ e.getMessage());
			}
			// 2.cleaning up the system and adding a node
			activeServers.remove(failedNode);
			activeConnections.remove(failedNode);
			calculateMetaData(activeServers);
			sendMetaData();
			addNode();

			logger.debug("$$$$$$ New SYSTEM STATE $$$$$$");
			for (ServerInfo s : activeServers)
				logger.debug(s.getAddress() + ":" + s.getPort());
			logger.debug("$$$$$$");

			return;
		} else if (activeServers.size() == 3) {
			logger.debug("ONE NODE FAILED! 2 NODES STILL RUNNING");
			/*
			 * 1. send a message to the successor to store replicated data of
			 * the failed node as its own data
			 */
			ServerConnection serverChannel = activeConnections
					.get(getSuccessor(failedNode));
			RecoverMessage recoverData = new RecoverMessage();
			recoverData.setActionType(ECSCommand.RECOVER_DATA);
			recoverData.setFailedServer(failedNode);
			try {
				serverChannel.connect();
				logger.debug("sending recover data to "
						+ serverChannel.getServer().getPort());
				serverChannel.sendMessage(recoverData);
				RecoverMessage temp = (RecoverMessage) SerializationUtil
						.toObject(SerializationUtil.toByteArray(recoverData));
				logger.debug(temp.getFailedServer().getPort());
				logger.debug(temp.getMessageType());
				serverChannel.disconnect();
			} catch (IOException e) {
				logger.error("recover data message couldn't be sent."
						+ e.getMessage());
			}

			// 2. removing the failed node and updating metadata
			activeServers.remove(failedNode);
			activeConnections.remove(failedNode);
			calculateMetaData(activeServers);
			sendMetaData();

			/*
			 * 3. tell the remaining servers to send their data as replication
			 * to each other
			 */
			sendData(activeServers.get(0), activeServers.get(1), activeServers
					.get(0).getFromIndex(), activeServers.get(0).getToIndex());
			sendData(activeServers.get(1), activeServers.get(0), activeServers
					.get(1).getFromIndex(), activeServers.get(1).getToIndex());

			locked.add(activeConnections.get(activeServers.get(0)));
			locked.add(activeConnections.get(activeServers.get(1)));

			logger.debug("Data for replication sent from "
					+ activeServers.get(0).getPort() + " to "
					+ activeServers.get(1).getPort());
			logger.debug("Data sent for replication from "
					+ activeServers.get(1).getPort() + " to "
					+ activeServers.get(0).getPort());
		}

		// normal cases when we have more than 3 servers before removing the
		// failed node
		else {
			ServerInfo successor = getSuccessor(failedNode);
			List<ServerInfo> masters = getMasters(failedNode);
			List<ServerInfo> replicas = getReplicas(failedNode);

			/*
			 * 1. send a message to the successor to store replicated data of
			 * the failed node as its own data
			 */
			ServerConnection serverChannel = activeConnections
					.get(getSuccessor(failedNode));
			RecoverMessage recoverData = new RecoverMessage();
			recoverData.setActionType(ECSCommand.RECOVER_DATA);
			recoverData.setFailedServer(failedNode);
			try {
				serverChannel.connect();
				logger.debug("sending recover data to "
						+ serverChannel.getServer().getPort());
				serverChannel.sendMessage(recoverData);
				serverChannel.disconnect();
				logger.info("Data recovered from replica "
						+ replicas.get(0).getPort() + " sent to "
						+ successor.getAddress() + ":" + successor.getPort());
			} catch (IOException e) {
				logger.error("recover data message couldn't be sent."
						+ e.getMessage());
				/*
				 * 1'. recovering data from successor failed, so recovering lost
				 * data from the other replicas
				 */
				if (sendData(replicas.get(1), successor,
						failedNode.getFromIndex(), failedNode.getToIndex()) == 0) {
					logger.info("Data recovered from replica "
							+ replicas.get(1).getAddress() + ":"
							+ replicas.get(1).getPort() + " sent to "
							+ successor.getPort());
					logger.info("Data recovered from replica "
							+ replicas.get(0).getAddress() + ":"
							+ replicas.get(0).getPort() + " sent to "
							+ successor.getAddress() + ":"
							+ successor.getPort());

				} else
					logger.warn(" Data owned by " + failedNode.getAddress()
							+ ":" + failedNode.getPort()
							+ " could not be recovered");
			}

			// 2. sending new metaData
			activeServers.remove(failedNode);
			activeConnections.remove(failedNode);
			calculateMetaData(activeServers);
			sendMetaData();

			/*
			 * 3. storing data as replication : sent from failed node's masters
			 * to failed node's replicas sent from successor to its replicas
			 */
			if (sendData(masters.get(0), replicas.get(0), masters.get(0)
					.getFromIndex(), masters.get(0).getToIndex()) == 0)
				logger.debug("Data for replication sent from "
						+ masters.get(0).getPort() + " to "
						+ replicas.get(0).getPort());
			else
				logger.warn("Data transfer for replication  from "
						+ masters.get(0).getPort() + " to "
						+ replicas.get(0).getPort() + " FAILED");

			if (sendData(masters.get(1), replicas.get(1), masters.get(1)
					.getFromIndex(), masters.get(1).getToIndex()) == 0)
				logger.debug("Data sent for replication from "
						+ masters.get(1).getPort() + " to "
						+ replicas.get(1).getPort());
			else
				logger.warn("Data transfer for replication  from "
						+ masters.get(1).getPort() + " to "
						+ replicas.get(1).getPort() + " FAILED");

			if (sendData(successor, successor.getFirstReplicaInfo(),
					failedNode.getFromIndex(), successor.getToIndex()) == 0)
				logger.debug("Data sent for replication from "
						+ masters.get(1).getPort() + " to "
						+ replicas.get(1).getPort());
			else
				logger.warn("Data transfer for replication  from "
						+ masters.get(1).getPort() + " to "
						+ replicas.get(1).getPort() + " FAILED");

			if (sendData(successor, successor.getSecondReplicaInfo(),
					failedNode.getFromIndex(), successor.getToIndex()) == 0)
				logger.debug("Data sent for replication from "
						+ successor.getPort() + " to "
						+ successor.getFirstReplicaInfo());
			else
				logger.warn("Data transfer for replication  from "
						+ successor.getPort() + " to "
						+ successor.getSecondReplicaInfo() + " FAILED");

			locked.add(activeConnections.get(successor));
			locked.add(activeConnections.get(masters.get(0)));
			locked.add(activeConnections.get(masters.get(1)));
		}

		// 4. releasing all locks
		logger.debug("releasing all locks!! ");
		ECSMessage releaseLock = new ECSMessage();
		releaseLock.setActionType(ECSCommand.RELEASE_LOCK);
		try {
			for (ServerConnection sChannel : locked) {
				sendECSCommand(sChannel, releaseLock);
				logger.debug("lock on " + sChannel.getServer().getPort()
						+ " released");
			}
		} catch (IOException e) {
			logger.error("ReLease Lock message couldn't be sent.");
		}

		// 5. adding a new node to the system
		addNode();

		logger.debug("$$$$$$ New SYSTEM STATE $$$$$$");
		for (ServerInfo s : activeServers)
			logger.debug(s.getAddress() + ":" + s.getPort());
		logger.debug("$$$$$$");

	}

	/**
	 * tells the sender to send data in range(fromIndex,toIndex) to the reciever
	 * 
	 * @param Sender
	 *            : (ServerInfo)The KVServer that sends the data
	 * @param reciever
	 *            : (ServerINfo) The KVServer that recieves the data
	 * @param fromIndex
	 * @param toIndex
	 * @return 0 in case of success and -1 otherwise
	 */
	private int sendData(ServerInfo sender, ServerInfo reciever,
			String fromIndex, String toIndex) {

		// in order to handle concurrent send from the same serverConnection I
		// used new objects!
		ServerConnection senderChannel = new ServerConnection(sender);
		ECSMessage writeLock = new ECSMessage();
		writeLock.setActionType(ECSCommand.SET_WRITE_LOCK);
		try {
			senderChannel.connect();
			senderChannel.sendMessage(writeLock);
		} catch (IOException e) {
			logger.error("WriteLock message couldn't be sent to "
					+ sender.getPort());
			senderChannel.disconnect();
			return -1;
		}

		logger.debug("Sender node " + sender + " reciever node " + reciever);
		try {
			Thread.sleep(200);
		} catch (InterruptedException e1) {
		}

		// Invoke the transfer of the affected data items
		ECSMessage moveDataMessage = new ECSMessage();
		moveDataMessage.setActionType(ECSCommand.MOVE_DATA);
		moveDataMessage.setMoveFromIndex(fromIndex);
		moveDataMessage.setMoveToIndex(toIndex);
		moveDataMessage.setMoveToServer(reciever);
		try {
			senderChannel.sendMessage(moveDataMessage);
			Thread temp = new Thread(senderChannel);
			temp.start();
			// 3000 is timeout
			synchronized (temp) {
				temp.wait(3000);
			}
			// sender channel got the Ack message
			if (senderChannel.gotResponse()) {
				senderChannel.setResponse(false);
				senderChannel.disconnect();

				return 0;
			} else {
				// sender channel could not got the Ack
				logger.warn(" TimeOut reached ! could not recieve Message from "
						+ senderChannel.getServer().getPort());
				senderChannel.disconnect();
				senderChannel.setResponse(false);
				return -1;
			}

		} catch (IOException e) {
			logger.error("MoveData message couldn't be sent to  "
					+ sender.getPort());
			senderChannel.disconnect();
			return -1;
		} catch (InterruptedException e) {
			logger.error("MoveData message couldn't be sent to  "
					+ sender.getPort());
			senderChannel.disconnect();
			return -1;
		}
	}

	/**
	 * tell the Server s to remove one of its replication storages
	 */
	private int removeData(ServerInfo server, String fromIndex, String toIndex) {
		// Invoke the removal of the replicated data items
		ECSMessage removeDataMessage = new ECSMessage();
		removeDataMessage.setActionType(ECSCommand.REMOVE_DATA);
		removeDataMessage.setMoveFromIndex(fromIndex);
		removeDataMessage.setMoveToIndex(toIndex);

		try {
			/*
			 * in order to handle concurrent send from the same serverConnection
			 * I used new objects!
			 */
			ServerConnection serverChannel = new ServerConnection(server);
			serverChannel.connect();
			serverChannel.sendMessage(removeDataMessage);
			Thread temp = new Thread(serverChannel);
			temp.start();
			// 3000 is timeout
			synchronized (temp) {
				temp.wait(3000);
			}
			// sender channel got the Ack message
			if (serverChannel.gotResponse()) {
				serverChannel.setResponse(false);
				serverChannel.disconnect();

				return 0;
			} else {
				logger.warn(" TimeOut reached ! could not recieve Message from "
						+ serverChannel.getServer().getPort());
				serverChannel.disconnect();
				serverChannel.setResponse(false);
				return -1;
			}
		} catch (IOException e) {
			logger.error("RemoveData message couldn't be sent to  "
					+ server.getAddress() + ":" + server.getPort());
			return -1;
		} catch (InterruptedException e) {
			logger.error("RemoveData message couldn't be sent to  "
					+ server.getAddress() + ":" + server.getPort());
			return -1;
		}
	}

	/**
	 * send metaData to the activeServers (in case of initialization or metaData
	 * Update)
	 * 
	 * @return
	 */
	private int sendMetaData() {
		// send meta data
		ECSMessage metaDataUpdate = new ECSMessage();
		metaDataUpdate.setActionType(ECSCommand.SEND_METADATA);
		metaDataUpdate.setMetaData(activeServers);
		for (ServerInfo server : this.activeServers) {
			try {
				ServerConnection serverChannel = activeConnections.get(server);
				serverChannel.connect();
				serverChannel.sendMessage(metaDataUpdate);
				serverChannel.disconnect();
			} catch (IOException e) {
				logger.error("Could not send message to server" + server
						+ e.getMessage());
				return -1;
			}
		}

		logger.debug("Updated Meta-data handed to servers.");
		return 0;
	}

	/**
	 * clean up the system metaData whenever a node should be deleted from the
	 * system
	 * 
	 * @param nodeToDelete
	 */
	private void cleanUpNode(ServerInfo nodeToDelete) {
		for (ServerInfo server : this.serverRepository) {
			if (server.equals(nodeToDelete)) {
				server.setServerLaunched(false);
				this.activeConnections.remove(nodeToDelete);
				this.activeServers.remove(nodeToDelete);
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

	private void sendECSCommand(ServerConnection channel, ECSMessage message)
			throws IOException {
		channel.connect();
		channel.sendMessage(message);
		channel.disconnect();
	}

	/**
	 * this method is called whenever a server has reports a failure to ECS
	 * 
	 * @param failedServer
	 * @param reportee
	 */
	public synchronized void reportFailure(ServerInfo failedServer,
			ServerInfo reportee) {
		logger.debug("Failure detected Failed:" + failedServer + " reporter:"
				+ reportee);
		try {
			Iterator<ServerInfo> activeServerIt = activeServers.iterator();
			while (activeServerIt.hasNext()) {
				ServerInfo serverInfo = activeServerIt.next();
				if (serverInfo.equals(failedServer)) {
					serverInfo.reportFailure(reportee);
				}
				if (activeServers.size() == 2) {
					recovery(failedServer);
					logger.debug("Failure detected more than one server reported");
					serverInfo.failureReportees.clear();
					notify();
				} else if (serverInfo.getNumberofFailReports() > 1) {
					recovery(failedServer);
					logger.debug("Failure detected more than one server reported");
					serverInfo.failureReportees.clear();
					notify();
				}
			}
		} catch (Exception e) {
			logger.debug(e.toString());
			notify();
		}
	}

}