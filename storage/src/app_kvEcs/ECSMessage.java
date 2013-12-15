package app_kvEcs;

import java.util.List;

import common.ServerInfo;
import common.messages.AbstractMessage;

public class ECSMessage implements AbstractMessage {

    private ECSCommand actionType;
    private List<ServerInfo> metaData;
    private String moveFromIndex;
    private String moveToIndex;
    private ServerInfo moveToServer;

    @Override
    public MessageType getMessageType() {
	return MessageType.ECS_MESSAGE;
    }

    public ECSCommand getActionType() {
	return actionType;
    }

    public void setActionType(ECSCommand actionType) {
	this.actionType = actionType;
    }

    public List<ServerInfo> getMetaData() {
	return metaData;
    }

    public void setMetaData(List<ServerInfo> metaData) {
	this.metaData = metaData;
    }

    public String getMoveFromIndex() {
	return moveFromIndex;
    }

    public void setMoveFromIndex(String moveFromIndex) {
	this.moveFromIndex = moveFromIndex;
    }

    public String getMoveToIndex() {
	return moveToIndex;
    }

    public void setMoveToIndex(String moveToIndex) {
	this.moveToIndex = moveToIndex;
    }

    public ServerInfo getMoveToServer() {
	return moveToServer;
    }

    public void setMoveToServer(ServerInfo moveToServer) {
	this.moveToServer = moveToServer;
    }

}
