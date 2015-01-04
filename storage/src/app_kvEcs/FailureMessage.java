package app_kvEcs;

import common.ServerInfo;
import common.messages.AbstractMessage;


public class FailureMessage implements AbstractMessage {

	ServerInfo failedServer;
	@Override
	public MessageType getMessageType() {
		return MessageType.FAILURE_DETECTION;
	}
	
	public ServerInfo getFailedServer(){
		return this.failedServer;
	}
	public void setFailedServer(ServerInfo value){
		this.failedServer = value;
	}

}
