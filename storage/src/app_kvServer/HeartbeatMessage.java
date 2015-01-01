package app_kvServer;

import common.ServerInfo;
import common.messages.AbstractMessage;

public class HeartbeatMessage implements AbstractMessage{

	private ServerInfo coordinatorServer;
	
	public HeartbeatMessage(ServerInfo coordinatorServer) {
		this.coordinatorServer = coordinatorServer;
	}
	
	public HeartbeatMessage(){
		
	}
	
	public void setCoordinatorServer(ServerInfo coordinatorServer) {
		this.coordinatorServer = coordinatorServer;
	}
	
	public ServerInfo getCoordinatorServer (){
		return this.coordinatorServer;
	}
	 
	@Override
	public MessageType getMessageType() {
		return MessageType.HEARTBEAT_MESSAGE;
	}

}
