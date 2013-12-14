package common.messages;

import java.util.List;
import java.util.Map;

import common.ServerInfo;

public class ClientMessage implements KVMessage {

	private String key;
	private String value;
	private StatusType type;
	private List < ServerInfo > metadata;

	public ClientMessage () {

	}

	public ClientMessage ( String key , String value , StatusType type ) {
		this.key = key;
		this.value = value;
		this.type = type;
	}

	public ClientMessage ( KVMessage message ) {
		this.key = message.getKey ();
		this.value = message.getValue ();
		this.type = message.getStatus ();
	}

	public void setKey ( String key ) {
		this.key = key;
	}

	public void setValue ( String value ) {
		this.value = value;
	}

	public void setStatus ( StatusType type ) {
		this.type = type;
	}

	@Override
	public String getKey () {
		return this.key;
	}

	@Override
	public String getValue () {
		return this.value;
	}

	@Override
	public StatusType getStatus () {
		return this.type;
	}

	public void setMetadata ( List < ServerInfo > metadata ) {
		this.metadata = metadata;
	}

	public List < ServerInfo > getMetadata () {
		return this.metadata;
	}

	@Override
	public MessageType getMessageType () {
		return MessageType.CLIENT_MESSAGE;
	}

}
