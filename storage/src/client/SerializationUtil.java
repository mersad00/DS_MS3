package client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.activation.UnsupportedDataTypeException;

import app_kvEcs.ECSCommand;
import app_kvEcs.ECSMessage;
import app_kvServer.HeartbeatMessage;
import app_kvServer.ReplicaMessage;
import app_kvServer.ServerMessage;
import common.ServerInfo;
import common.messages.AbstractMessage;
import common.messages.AbstractMessage.MessageType;
import common.messages.KVMessage.StatusType;
import common.messages.ClientMessage;

public class SerializationUtil {

    private static final String LINE_FEED = "&&";
    private static final String INNER_LINE_FEED = "##";
    private static final String INNER_LINE_FEED2 = "%%";
    private static final String INNER_LINE_FEED3 = "@@";
    private static final String ECS_MESSAGE = "0";
    private static final String SERVER_MESSAGE = "1";
    private static final String CLIENT_MESSAGE = "2";
    private static final String REPLICA_MESSAGE = "3";
    private static final String HEARTBEAT_MESSAGE = "4";

    private static final char RETURN = 0x0D;

    public static byte[] toByteArray(ClientMessage message) {

	// message : number(0)$key$value

	String messageStr = (CLIENT_MESSAGE+LINE_FEED+message.getStatus().ordinal() + LINE_FEED
		+ message.getKey() + LINE_FEED + message.getValue());

	if (message.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE){
	    // add metadata
	    messageStr += LINE_FEED;
	    for (ServerInfo server : message.getMetadata()) {
		messageStr += server.getAddress()+INNER_LINE_FEED+server.getPort()+INNER_LINE_FEED+server.getFromIndex()+INNER_LINE_FEED+server.getToIndex();
		messageStr+=INNER_LINE_FEED2;
	    }
	}
	byte[] bytes =messageStr.getBytes();
	byte[] ctrBytes = new byte[] { RETURN };
	byte[] tmp = new byte[bytes.length + ctrBytes.length];
	System.arraycopy(bytes, 0, tmp, 0, bytes.length);
	System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
	return tmp;
    }

    public static AbstractMessage toObject(byte[] objectByteStream) throws UnsupportedDataTypeException {

	String message = new String(objectByteStream).trim();
	String[] tokens = message.split(LINE_FEED);
	AbstractMessage retrivedMessage = null;
	// 0 is the message type[0,1,2]
	if (tokens[0] != null) {
	    AbstractMessage.MessageType messageType = toMessageType(tokens[0]);
	    switch (messageType) {
	    case CLIENT_MESSAGE:
		 retrivedMessage = new ClientMessage();
		// 1: is always the status
		// 2: could be the key value or error
		// 3: could be value or error
		if (tokens[1] != null) {// should always be the status
		    int statusOrdinal = Integer.parseInt(tokens[1]);
		    ((ClientMessage)retrivedMessage).setStatus(StatusType.values()[statusOrdinal]);
		}
		if (tokens[2] != null) {// is always the key
		    ((ClientMessage)retrivedMessage).setKey(tokens[2]);

		}
		if (tokens[3] != null) {
		    ((ClientMessage)retrivedMessage).setValue(tokens[3].trim());
		}if (tokens.length>= 5 && tokens[4] != null){
		    List<ServerInfo> metaData = getMetaData(tokens[4].trim());
		    ((ClientMessage)retrivedMessage).setMetadata(metaData);
		}
		break;
	    case SERVER_MESSAGE:
		 retrivedMessage = new ServerMessage();
			// 1: is always the status
			// 2: could be the key value or error
			// 3: could be value or error
			if (tokens[1] != null && tokens[1].equals("0") ) {// should always be 0
				//TODO avoid exception in case of empty map attahed
			  Map<String, String> data = getData(tokens[2].trim());
			  ((ServerMessage)retrivedMessage).setData(data);
			  }
			
		break;
	    case ECS_MESSAGE:
		 retrivedMessage = new ECSMessage();
			// 1: is always the action
			// 2: could be the  metadata or from
			// 3: could be to index
		 	// 4: could be the to server
		        // 5 : could be the to port
			if (tokens.length>= 2 && tokens[1] != null) {// should always be the action
			    int actionOrdinal = Integer.parseInt(tokens[1]);
			    ((ECSMessage)retrivedMessage).setActionType(ECSCommand.values()[actionOrdinal]);
			}
			if (tokens.length>= 3 && tokens[2] != null) {// is always the key
			    if(((ECSMessage)retrivedMessage).getActionType()== (ECSCommand.INIT) ||
				    ((ECSMessage)retrivedMessage).getActionType()==(ECSCommand.SEND_METADATA)){
				List<ServerInfo> metaData = getMetaData(tokens[2].trim());
				((ECSMessage)retrivedMessage).setMetaData(metaData);
			    }else  if(((ECSMessage)retrivedMessage).getActionType() == (ECSCommand.MOVE_DATA)){ 
				((ECSMessage)retrivedMessage).setMoveFromIndex(tokens[2].trim());
			    }
			    
			}
			if (tokens.length>= 4 && tokens[3] != null) {// to index
			    ((ECSMessage)retrivedMessage).setMoveToIndex(tokens[3].trim());
			}
			if (tokens.length>= 6 && tokens[4] != null && tokens[5] != null ) {
			    ServerInfo toServer = new ServerInfo(tokens[4],Integer.parseInt(tokens[5]));
			    ((ECSMessage)retrivedMessage).setMoveToServer(toServer);
			}
			
		break;
		
	    case REPLICA_MESSAGE: {
			retrivedMessage = new ReplicaMessage();
			// 1: is always the status
			// 2: could be the key value or error
			// 3: could be value or error
			if (tokens[1] != null) {// should always be the status
				int statusOrdinal = Integer.parseInt(tokens[1]);
				((ReplicaMessage) retrivedMessage).setStatusType(StatusType
						.values()[statusOrdinal]);
			}
			if (tokens[2] != null) {// is always the key
				((ReplicaMessage) retrivedMessage).setKey(tokens[2]);

			}
			if (tokens[3] != null) {
				((ReplicaMessage) retrivedMessage).setValue(tokens[3]
						.trim());
			}
			if (tokens[4] != null) {
				ServerInfo coordinator = getServerInfo(tokens[4].trim());
				coordinator.setFirstReplicaInfo(getServerInfo(tokens[5].trim()));
				coordinator.setSecondReplicaInfo(getServerInfo(tokens[6].trim()));
				((ReplicaMessage) retrivedMessage)
						.setCoordinatorServer(coordinator);
			}
			
			break;
		}
	    
	    case HEARTBEAT_MESSAGE : {
	    	System.out.println(tokens[0]);
			System.out.println(tokens[1]);
			System.out.println(tokens[2]);
			System.out.println(tokens[3]);
	    	retrivedMessage = new HeartbeatMessage();
			
			if (tokens[1] != null) {
				ServerInfo coordinator = getServerInfo(tokens[1].trim());
				coordinator.setFirstReplicaInfo(getServerInfo(tokens[2].trim()));
				coordinator.setSecondReplicaInfo(getServerInfo(tokens[3].trim()));
				((HeartbeatMessage) retrivedMessage)
						.setCoordinatorServer(coordinator);
			}
	    	break;
	    }
	    default:
		    break;

	    }
	}
	
	return retrivedMessage;
    }

    private static Map<String, String> getData(String dataStr) {
	Map<String, String> data = new HashMap<String, String>();
	String[] tokens = dataStr.split(INNER_LINE_FEED2);
	for (String dataTuple : tokens) {
	    String[] dataTokens = dataTuple.split(INNER_LINE_FEED);
	    data.put(dataTokens[0], dataTokens[1]);
	}
	return data;
    }

    private static List<ServerInfo> getMetaData(String metaDataStr) {
		List<ServerInfo> serverInfoList = new ArrayList<ServerInfo>();
		String[] tokens = metaDataStr.split(INNER_LINE_FEED2);
		for (String serverInfoStr : tokens) {
			String[] serverInfoTokens = serverInfoStr.split(INNER_LINE_FEED);
			ServerInfo serverInfo = new ServerInfo(serverInfoTokens[0],
					Integer.parseInt(serverInfoTokens[1]), serverInfoTokens[2],
					serverInfoTokens[3]);
			if (serverInfoTokens.length > 4) {
				String[] replicasInfo = serverInfoTokens[4]
						.split(INNER_LINE_FEED3);
				ServerInfo replica1 = new ServerInfo(replicasInfo[0],
						Integer.parseInt(replicasInfo[1]), replicasInfo[2],
						replicasInfo[3]);
				ServerInfo replica2 = new ServerInfo(replicasInfo[4],
						Integer.parseInt(replicasInfo[5]), replicasInfo[6],
						replicasInfo[7]);
				serverInfo.setFirstReplicaInfo(replica1);
				serverInfo.setSecondReplicaInfo(replica2);
			}
			serverInfoList.add(serverInfo);
		}

		return serverInfoList;
	}

    private static MessageType toMessageType(String messageTypeStr) throws UnsupportedDataTypeException {

	if (messageTypeStr.equals(CLIENT_MESSAGE))
	    return MessageType.CLIENT_MESSAGE;
	else if (messageTypeStr.equals(SERVER_MESSAGE))
	    return MessageType.SERVER_MESSAGE;
	else if (messageTypeStr.equals(ECS_MESSAGE))
	    return MessageType.ECS_MESSAGE;
	else if (messageTypeStr.equals(REPLICA_MESSAGE))
		return MessageType.REPLICA_MESSAGE;
	else if (messageTypeStr.equals(HEARTBEAT_MESSAGE))
		return MessageType.HEARTBEAT_MESSAGE;
	else 
	    throw new UnsupportedDataTypeException("Unsupported message type");

    }

    public static byte[] toByteArray(ServerMessage message) {

	// message : number(messageType)-- number(actionType)--map of data
	String messageStr = (SERVER_MESSAGE+LINE_FEED+"0");// the only action is move data =0
	    messageStr += LINE_FEED;
	    
	    Iterator it = message.getData().entrySet().iterator();
	    while (it.hasNext()) {
	        Entry pairs = (Entry)it.next();
	        messageStr += pairs.getKey()+INNER_LINE_FEED+pairs.getValue();
		messageStr+=INNER_LINE_FEED2;
	    }

	byte[] bytes = messageStr.getBytes();
	byte[] ctrBytes = new byte[] { RETURN };
	byte[] tmp = new byte[bytes.length + ctrBytes.length];
	System.arraycopy(bytes, 0, tmp, 0, bytes.length);
	System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
	return tmp;

    }

    public static byte[] toByteArray(ECSMessage message) {
		// message : number(messageType)-- number(actionType)--list of meta
		// data/fromindex--toindex-- to_serverInfo
		String messageStr = (ECS_MESSAGE + LINE_FEED + message.getActionType()
				.ordinal());
		if (message.getActionType() == ECSCommand.INIT
				|| message.getActionType() == ECSCommand.SEND_METADATA) {
			// add metadata
			messageStr += LINE_FEED;
			for (ServerInfo server : message.getMetaData()) {
				messageStr += server.getAddress() + INNER_LINE_FEED
						+ server.getPort() + INNER_LINE_FEED
						+ server.getFromIndex() + INNER_LINE_FEED
						+ server.getToIndex() + INNER_LINE_FEED
						+ server.getFirstReplicaInfo().getAddress()
						+ INNER_LINE_FEED3
						+ server.getFirstReplicaInfo().getPort()
						+ INNER_LINE_FEED3
						+ server.getFirstReplicaInfo().getFromIndex()
						+ INNER_LINE_FEED3
						+ server.getFirstReplicaInfo().getToIndex()
						+ INNER_LINE_FEED3
						+ server.getSecondReplicaInfo().getAddress()
						+ INNER_LINE_FEED3
						+ server.getSecondReplicaInfo().getPort()
						+ INNER_LINE_FEED3
						+ server.getSecondReplicaInfo().getFromIndex()
						+ INNER_LINE_FEED3
						+ server.getSecondReplicaInfo().getToIndex()
						+ INNER_LINE_FEED;
				messageStr += INNER_LINE_FEED2;
			}

		} else if (message.getActionType() == ECSCommand.MOVE_DATA) {
			// add the from and to and the server info
			ServerInfo server = message.getMoveToServer();
			messageStr += LINE_FEED + message.getMoveFromIndex() + LINE_FEED
					+ message.getMoveToIndex() + LINE_FEED
					+ server.getAddress() + LINE_FEED + server.getPort();

		}
		
		byte[] bytes = messageStr.getBytes();
		byte[] ctrBytes = new byte[] { RETURN };
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		return tmp;
    }
    
    public static byte[] toByteArray(ReplicaMessage message) {
    	
		String messageStr = (REPLICA_MESSAGE + LINE_FEED
				+ message.getStatus().ordinal() + LINE_FEED + message.getKey()
				+ LINE_FEED + message.getValue() + LINE_FEED
				+ message.getCoordinatorServerInfo().getAddress()
				+ INNER_LINE_FEED
				+ message.getCoordinatorServerInfo().getPort()
				+ INNER_LINE_FEED
				+ message.getCoordinatorServerInfo().getFromIndex()
				+ INNER_LINE_FEED + message.getCoordinatorServerInfo()
				.getToIndex())
				+ LINE_FEED + message.getCoordinatorServerInfo().getFirstReplicaInfo().getAddress() + INNER_LINE_FEED
				+ message.getCoordinatorServerInfo().getFirstReplicaInfo().getPort() + INNER_LINE_FEED
				+ message.getCoordinatorServerInfo().getFirstReplicaInfo().getFromIndex() + INNER_LINE_FEED
				+ message.getCoordinatorServerInfo().getFirstReplicaInfo().getToIndex() + INNER_LINE_FEED
				+ LINE_FEED + message.getCoordinatorServerInfo().getSecondReplicaInfo().getAddress() + INNER_LINE_FEED
				+ message.getCoordinatorServerInfo().getSecondReplicaInfo().getPort() + INNER_LINE_FEED
				+ message.getCoordinatorServerInfo().getSecondReplicaInfo().getFromIndex() + INNER_LINE_FEED
				+ message.getCoordinatorServerInfo().getSecondReplicaInfo().getToIndex() + INNER_LINE_FEED;
		
		
		byte[] bytes = messageStr.getBytes();
		byte[] ctrBytes = new byte[] { RETURN };
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		return tmp;
	}

    private static ServerInfo getServerInfo(String serverInfoStr) {
		ServerInfo serverInfo = new ServerInfo();
		String[] serverInfoTokens = serverInfoStr.split(INNER_LINE_FEED);
		serverInfo = new ServerInfo(serverInfoTokens[0],
				Integer.parseInt(serverInfoTokens[1]), serverInfoTokens[2],
				serverInfoTokens[3]);

		return serverInfo;
	}

    public static byte[] toByteArray(HeartbeatMessage message){
    	String messageStr = (HEARTBEAT_MESSAGE + LINE_FEED				
				+ message.getCoordinatorServer().getAddress()
				+ INNER_LINE_FEED
				+ message.getCoordinatorServer().getPort()
				+ INNER_LINE_FEED
				+ message.getCoordinatorServer().getFromIndex()
				+ INNER_LINE_FEED + message.getCoordinatorServer()
				.getToIndex())
				+ LINE_FEED + message.getCoordinatorServer().getFirstReplicaInfo().getAddress() + INNER_LINE_FEED
				+ message.getCoordinatorServer().getFirstReplicaInfo().getPort() + INNER_LINE_FEED
				+ message.getCoordinatorServer().getFirstReplicaInfo().getFromIndex() + INNER_LINE_FEED
				+ message.getCoordinatorServer().getFirstReplicaInfo().getToIndex() + INNER_LINE_FEED
				+ LINE_FEED + message.getCoordinatorServer().getSecondReplicaInfo().getAddress() + INNER_LINE_FEED
				+ message.getCoordinatorServer().getSecondReplicaInfo().getPort() + INNER_LINE_FEED
				+ message.getCoordinatorServer().getSecondReplicaInfo().getFromIndex() + INNER_LINE_FEED
				+ message.getCoordinatorServer().getSecondReplicaInfo().getToIndex();
    	
    	byte[] bytes = messageStr.getBytes();
		byte[] ctrBytes = new byte[] { RETURN };
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		return tmp;
    }
}
