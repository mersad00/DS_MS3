package app_kvServer;

import common.messages.AbstractMessage;

public class ServerMessage implements AbstractMessage{

	
	@Override
	public MessageType getMessageType () {
		return MessageType.SERVER_MESSAGE;
	}

}
