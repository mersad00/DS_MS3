package app_kvEcs;

import common.ServerInfo;
import common.messages.AbstractMessage;

/**
 * 
 * Class of Recovery Message, sent by ecs to servers to recover portion of data
 * from their replica storage
 */
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

	public ServerInfo getFailedServer() {
		return failedServer;
	}

	public void setFailedServer(ServerInfo failed) {
		this.failedServer = failed;
	}
}
