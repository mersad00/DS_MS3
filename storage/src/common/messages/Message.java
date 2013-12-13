package common.messages;

import java.util.Map;

import common.ServerInfo;

public class Message implements KVMessage {

	private String key;
	private String value;
	private StatusType type;
	private Map < String , ServerInfo > metadata;

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

	public void setMetadata ( Map < String , ServerInfo > metadata ) {
		this.metadata = metadata;
	}

	public Map < String , ServerInfo > getMetadata () {
		return this.metadata;
	}

}
