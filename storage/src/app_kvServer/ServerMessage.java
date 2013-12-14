package app_kvServer;

import java.util.Map;

import common.messages.AbstractMessage;

public class ServerMessage implements AbstractMessage {

	private Map < String , String > data;

	@Override
	public MessageType getMessageType () {
		return MessageType.SERVER_MESSAGE;
	}

	public Map < String , String > getData () {
		return this.data;
	}

	public void setData ( Map < String , String > data ) {
		this.data = data;
	}

}
