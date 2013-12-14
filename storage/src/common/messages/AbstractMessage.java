package common.messages;

public interface AbstractMessage {

	public enum MessageType {
		CLIENT_MESSAGE, SERVER_MESSAGE, ECS_MESSAGE
	}
	
	public abstract MessageType getMessageType ();
}
