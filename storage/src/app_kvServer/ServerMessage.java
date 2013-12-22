/**
 * This class is the representation of the server message
 * which should be sent between servers to move data from
 * server to another server.
 * 
 * @see AbstractMessage
 * @see MessageType
 */
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
