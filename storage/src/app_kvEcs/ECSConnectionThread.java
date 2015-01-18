package app_kvEcs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import org.apache.log4j.Logger;
import client.SerializationUtil;
import common.messages.AbstractMessage;
import common.messages.AbstractMessage.MessageType;
import utilities.LoggingManager;

/**
 * This Class is responsible for getting report failures from the servers
 *
 */

public class ECSConnectionThread implements Runnable {
	private static Logger logger;

	private boolean isOpen;

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	private ECSImpl parent;
	
	public ECSConnectionThread(Socket client, ECSImpl ecs){
		this.clientSocket = client;
		this.parent = ecs;
		logger = LoggingManager.getInstance().createLogger(this.getClass());
		isOpen =true;
	}
	@Override
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();

			while (isOpen) {
				try {
					AbstractMessage msg = receiveMessage();
					
					logger.debug("FAILURE REPORTER");
					
					handleRequest(msg); // to determine the connection type
					/*
					 * connection either terminated by the client or lost due to
					 * network problems
					 */
				} catch (IOException ioe) {
					//logger.error("Error! Connection lost!"
					//		+ ioe.getStackTrace());
					logger.debug("Error! Connection lost!"
							+ ioe.getMessage());
					
					isOpen = false;
				}
			}

		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);

		} finally {

			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}
	
	
	private AbstractMessage receiveMessage() throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;
		if (read == -1) {
			throw new IOException();
		}
		while (read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				if (msgBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			/* only read valid characters, i.e. letters and numbers */
			if ((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}

			/* stop reading is DROP_SIZE is reached */
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}

			/* read next char from stream */
			read = (byte) input.read();
		}

		if (msgBytes == null) {
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}

		msgBytes = tmp;

		/* build final Message */
		AbstractMessage msg = SerializationUtil.toObject(msgBytes);
		logger.info("Receive message:\t '" + msg.getMessageType() + "'");
		return msg;
	}

	private void handleRequest(AbstractMessage msg) throws IOException {
		if (msg.getMessageType().equals(MessageType.FAILURE_DETECTION)) {
			FailureMessage fmsg = (FailureMessage)msg;
			parent.reportFailure(fmsg.getFailedServer(),fmsg.getReporteeServer());
		} 
	}

	
}
