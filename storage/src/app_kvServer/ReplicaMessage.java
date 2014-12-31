package app_kvServer;

import common.ServerInfo;
import common.messages.KVMessage;

public class ReplicaMessage implements KVMessage {

	private StatusType statusType;
	private String key;
	private String value;
	private ServerInfo coordinatorServer; /* the master server which sends the replica data to replicas */

	@Override
	public MessageType getMessageType() {
		return MessageType.REPLICA_MESSAGE;
	}

	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public String getValue() {
		return this.value;
	}

	@Override
	public StatusType getStatus() {
		return this.statusType;
	}

	public ServerInfo getCoordinatorServerInfo() {
		return this.coordinatorServer;
	}

	public void setStatusType(StatusType type) {
		this.statusType = type;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public void setCoordinatorServer(ServerInfo serverInfo) {
		this.coordinatorServer = serverInfo;
	}

}
