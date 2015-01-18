package app_kvEcs;

import common.ServerInfo;
import common.messages.AbstractMessage;
/**
 * Special Class for FailureMessages, which are sent by servers 
 *
 */

public class FailureMessage implements AbstractMessage {

	ServerInfo failedServer;
	ServerInfo reporteeServer;
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
	
	public ServerInfo getReporteeServer(){
		return this.reporteeServer;
	}
	public void setReporteeServer(ServerInfo value){
		this.reporteeServer = value;
	}


}
