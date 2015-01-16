package common.messages;

import client.ClientInfo;

public class UnsubscribeMessage implements AbstractMessage{

	private String key;
	private ClientInfo subscriber;
	
	
	public void setKey(String key){
		this.key = key;
	}
	
	public String getKey(){
		return this.key;
	}
	
	public void setSubscriber(ClientInfo subscriber){
		this.subscriber = subscriber;
	}
	
	public ClientInfo getSubscriber(){
		return this.subscriber;
	}
	
	
	@Override
	public MessageType getMessageType() {
		return MessageType.UNSUBSCRIBE_MESSAGE;
	}

}
