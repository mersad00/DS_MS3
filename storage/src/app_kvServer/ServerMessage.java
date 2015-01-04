/**
 * This class is the representation of the server message
 * which should be sent between servers to save data from
 * server to another server.
 * 
 * @see AbstractMessage
 * @see MessageType
 */
package app_kvServer;

import java.util.Map;

import common.messages.AbstractMessage;

public class ServerMessage implements AbstractMessage {

	private Map<String, String> data;
	private String saveFromIndex;
	private String saveToIndex;

	@Override
	public MessageType getMessageType() {
		return MessageType.SERVER_MESSAGE;
	}

	public Map<String, String> getData() {
		return this.data;
	}

	public void setData(Map<String, String> data) {
		this.data = data;
	}

	public String getSaveFromIndex() {
		return saveFromIndex;
	}

	public void setSaveFromIndex(String saveFromIndex) {
		this.saveFromIndex = saveFromIndex;
	}

	public String getSaveToIndex() {
		return saveToIndex;
	}

	public void setSaveToIndex(String saveToIndex) {
		this.saveToIndex = saveToIndex;
	}

}
