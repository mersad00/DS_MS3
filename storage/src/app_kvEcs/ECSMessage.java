package app_kvEcs;

import common.messages.AbstractMessage;

public class ECSMessage implements AbstractMessage {

	@Override
	public MessageType getMessageType () {
		return MessageType.ECS_MESSAGE;
	}

}
