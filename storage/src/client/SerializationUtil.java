package client;

import common.messages.KVMessage.StatusType;
import common.messages.KVMessage;
import common.messages.Message;

public class SerializationUtil {

	private static final String LINE_FEED = "&&";
	private static final char RETURN = 0x0D;

	public static byte[] toByteArray(KVMessage message) {

		// message : number(0)$key$value

		byte[] bytes = (message.getStatus().ordinal() + LINE_FEED
				+ message.getKey() + LINE_FEED + message.getValue()).getBytes();
		byte[] ctrBytes = new byte[] { RETURN };
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		return tmp;
	}

	public static KVMessage toObject(byte[] objectByteStream) {

		String message = new String(objectByteStream);
		String[] tokens = message.split(LINE_FEED);
		Message retrivedMessage = new Message();
		// 0: is always the status
		// 1: could be the key value or error
		// 2: could be value or error
		if (tokens[0] != null) {// should always be the status
			int statusOrdinal = Integer.parseInt(tokens[0]);
			retrivedMessage.setStatus(StatusType.values()[statusOrdinal]);
		}
		if (tokens[1] != null) {// is always the key
			retrivedMessage.setKey(tokens[1]);

		}
		if (tokens[2] != null) {
			retrivedMessage.setValue(tokens[2].trim());
		}
		return retrivedMessage;
	}

}
