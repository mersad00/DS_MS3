package common.messages;

public interface AbstractMessage {

	
	public enum MessageType {
		CLIENT_MESSAGE, SERVER_MESSAGE, ECS_MESSAGE, REPLICA_MESSAGE, HEARTBEAT_MESSAGE
	}
	
	/**
	 * 
	 * @return <code>MessageType</code> representing the message type
	 */
	public abstract MessageType getMessageType ();
}
