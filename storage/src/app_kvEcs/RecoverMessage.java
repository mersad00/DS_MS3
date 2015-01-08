package app_kvEcs;

import java.util.List;

import common.ServerInfo;
import common.messages.AbstractMessage;

public class RecoverMessage implements AbstractMessage {

    private ECSCommand actionType;
    private ServerInfo failedServer;
    
    @Override
    public MessageType getMessageType() {
	return MessageType.RECOVERY_MESSAGE;
    }

    public ECSCommand getActionType() {
	return actionType;
    }

    public void setActionType(ECSCommand actionType) {
	this.actionType = actionType;
    }
    
    public ServerInfo getFailedServer(){
    	return failedServer;
    }
    
    public void setFailedServer(ServerInfo failed){
    	this.failedServer = failed;
    }
}
