package common.messages;

import common.messages.KVMessage.StatusType;

import client.ClientInfo;

public class SubscribeMessage implements AbstractMessage {

	private ClientInfo subscriber;
	private String key;
	private String value;
	private StatusType statusType;
	
	public ClientInfo getSubscriber() {
		return subscriber;
	}

	public void setSubscriber(ClientInfo subscriber) {
		this.subscriber = subscriber;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public StatusType getStatusType() {
		return statusType;
	}

	public void setStatusType(StatusType statusType) {
		this.statusType = statusType;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.SUBSCRIBE_MESSAGE;
	}

}
