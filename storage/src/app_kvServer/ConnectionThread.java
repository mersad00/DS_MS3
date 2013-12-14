package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;

import org.apache.log4j.Logger;

import app_kvEcs.ECSMessage;

import client.SerializationUtil;

import common.messages.AbstractMessage;
import common.messages.KVMessage;
import common.messages.ClientMessage;
import common.messages.KVMessage.StatusType;

public class ConnectionThread implements Runnable {

	private static Logger logger = Logger.getRootLogger ();

	private boolean isOpen;

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	private KVServer parent;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * 
	 * @param clientSocket
	 *            the Socket object for the client connection.
	 */
	public ConnectionThread ( Socket clientSocket , KVServer parent ) {
		this.parent = parent;
		this.clientSocket = clientSocket;
		this.isOpen = true;
	}

	/**
	 * Initializes and starts the client connection. Loops until the connection
	 * is closed or aborted by the client.
	 */
	public void run () {
		try {
			output = clientSocket.getOutputStream ();
			input = clientSocket.getInputStream ();

			while ( isOpen ) {
				try {
					AbstractMessage msg = receiveMessage ();
					handleRequest ( msg );
					/*
					 * connection either terminated by the client or lost due to
					 * network problems
					 */
				} catch ( IOException ioe ) {
					logger.error ( "Error! Connection lost!" );
					isOpen = false;
				}
			}

		} catch ( IOException ioe ) {
			logger.error ( "Error! Connection could not be established!" , ioe );

		} finally {

			try {
				if ( clientSocket != null ) {
					input.close ();
					output.close ();
					clientSocket.close ();
				}
			} catch ( IOException ioe ) {
				logger.error ( "Error! Unable to tear down connection!" , ioe );
			}
		}
	}

	private AbstractMessage receiveMessage () throws IOException {

		int index = 0;
		byte [] msgBytes = null , tmp = null;
		byte [] bufferBytes = new byte [ BUFFER_SIZE ];

		/* read first char from stream */
		byte read = ( byte ) input.read ();
		boolean reading = true;
		if ( read == - 1 ) {
			throw new IOException ();
		}
		while ( read != 13 && reading ) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if ( index == BUFFER_SIZE ) {
				if ( msgBytes == null ) {
					tmp = new byte [ BUFFER_SIZE ];
					System.arraycopy ( bufferBytes , 0 , tmp , 0 , BUFFER_SIZE );
				} else {
					tmp = new byte [ msgBytes.length + BUFFER_SIZE ];
					System.arraycopy ( msgBytes , 0 , tmp , 0 , msgBytes.length );
					System.arraycopy ( bufferBytes , 0 , tmp , msgBytes.length ,
							BUFFER_SIZE );
				}

				msgBytes = tmp;
				bufferBytes = new byte [ BUFFER_SIZE ];
				index = 0;
			}

			/* only read valid characters, i.e. letters and numbers */
			if ( ( read > 31 && read < 127 ) ) {
				bufferBytes [ index ] = read;
				index++ ;
			}

			/* stop reading is DROP_SIZE is reached */
			if ( msgBytes != null && msgBytes.length + index >= DROP_SIZE ) {
				reading = false;
			}

			/* read next char from stream */
			read = ( byte ) input.read ();
		}

		if ( msgBytes == null ) {
			tmp = new byte [ index ];
			System.arraycopy ( bufferBytes , 0 , tmp , 0 , index );
		} else {
			tmp = new byte [ msgBytes.length + index ];
			System.arraycopy ( msgBytes , 0 , tmp , 0 , msgBytes.length );
			System.arraycopy ( bufferBytes , 0 , tmp , msgBytes.length , index );
		}

		msgBytes = tmp;

		/* build final Message */
		AbstractMessage msg = SerializationUtil.toObject ( msgBytes );
		logger.info ( "Receive message:\t '" + msg.getMessageType () + "'" );
		return msg;
	}

	private void sendMessage ( byte [] msgBytes ) throws IOException {
		output.write ( msgBytes , 0 , msgBytes.length );
		output.flush ();
	}

	private void sendClientMessage ( KVMessage msg ) throws IOException {
		byte [] msgBytes = SerializationUtil.toByteArray ( msg );
		sendMessage ( msgBytes );
		logger.info ( "Send client message:\t '" + msg.getKey () + "'" );
	}

	private void sendServerMessage ( ServerMessage msg ) throws IOException {
		byte [] msgBytes = SerializationUtil.toByteArray ( msg );
		sendMessage ( msgBytes );
		// TODO print useful data in the logger
		// logger.info ( "Send server message:\t '" + msg.getKey () + "'" );
	}

	private void sendECSMessage ( ECSMessage msg ) throws IOException {
		byte [] msgBytes = SerializationUtil.toByteArray ( msg );
		sendMessage ( msgBytes );
		// TODO print useful data in the logger
		// logger.info ( "Send client message:\t '" + msg.getKey () + "'" );
	}

	private void handleRequest ( AbstractMessage msg ) throws IOException {
		if ( msg instanceof ClientMessage ) {
			handleClientRequest ( ( ClientMessage ) msg );
		} else if ( msg instanceof ServerMessage ) {
			handleServerRequest ( ( ServerMessage ) msg );
		} else if ( msg instanceof ECSMessage ) {
			handleECSRequest ( ( ECSMessage ) msg );
		}

	}

	private void handleClientRequest ( ClientMessage msg ) throws IOException {
		KVMessage responseMessage = null;
		if ( parent.getServerStatus ().equals (
				ServerStatuses.UNDER_INITIALIZATION ) ) {
			// The server has just started and not ready yet to handle requests
			// from clients
			msg.setStatus ( StatusType.SERVER_STOPPED );
			responseMessage = new ClientMessage ( msg );

		} else if ( parent.getServerStatus ().equals (
				ServerStatuses.WRITING_LOCK ) ) {
			msg.setStatus ( StatusType.SERVER_WRITE_LOCK );
			responseMessage = new ClientMessage ( msg );

		} else if ( parent.getServerStatus ().equals ( ServerStatuses.ACTIVE ) ) {
			// The server is ready to handle requests
			if ( msg.getStatus ().equals ( KVMessage.StatusType.GET ) ) {
				responseMessage = DatabaseManager.get ( msg.getKey () );

			} else if ( msg.getStatus ().equals ( KVMessage.StatusType.PUT ) ) {
				responseMessage = DatabaseManager.put ( msg.getKey () ,
						msg.getValue () );
			}
		}
		this.sendClientMessage ( responseMessage );
	}

	private void handleServerRequest ( ServerMessage msg ) throws IOException {

	}

	private void handleECSRequest ( ECSMessage msg ) throws IOException {
		// TODO
	}
}
