package app_kvEcs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import client.KVStore;
import client.SerializationUtil;

import common.ServerInfo;

public class ServerConnection {

	private Logger logger = Logger.getRootLogger ();

	private Socket connection;
	private OutputStream output;
	private InputStream input;

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	private ServerInfo server;

	/**
	 * 
	 * Initialize Server Connection with address and port of KVServer
	 * 
	 * @param address
	 *            the address of the KVServer
	 * @param port
	 *            the port of the KVServer
	 */
	public ServerConnection ( String address , int port ) {
		this ( new ServerInfo ( address , port ) );
	}

	public ServerConnection ( ServerInfo serverInfo ) {
		this.server = serverInfo;
	}

	public void connect () throws IOException {
		try {
			connection = new Socket ( server.getAddress () , server.getPort () );
			output     = connection.getOutputStream ();
			input      = connection.getInputStream  ();
			
			logger.info ( "Connection established with " + server.toString () );
		} catch ( IOException ioe ) {

			logger.error ( "Connection could not be established!" );
			throw ioe;
		}
	}

	
	public void disconnect () {
		try {
			tearDownConnection ();
		} catch ( IOException ioe ) {
			logger.error ( "Unable to close connection!" );
		}
	}

	private void tearDownConnection () throws IOException {
		logger.info ( "tearing down the connection ..." );
		if ( connection != null ) {
			input.close ();
			output.close ();
			connection.close ();
			connection = null;

			logger.info ( "connection closed!" );
		}
	}

	

	private ECSMessage receiveMessage () throws IOException {
		int index = 0;
		byte [] msgBytes = null , tmp = null;
		byte [] bufferBytes = new byte [ BUFFER_SIZE ];

		/* read first char from stream */
		byte read = ( byte ) input.read ();
		boolean reading = true;

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

		/* build final String */
		ECSMessage msg = ( ECSMessage ) SerializationUtil.toObject ( msgBytes );
		logger.info ( "Receive ECSMessage :\t '" + msg.getActionType () + "'" );
		return msg;
	}

	private void sendMessage ( ECSMessage msg ) throws IOException {		
		byte [] msgBytes = SerializationUtil.toByteArray ( msg );
		output.write ( msgBytes , 0 , msgBytes.length );
		output.flush ();
		logger.info ( "Send message :\t '" + msg.getActionType () + "' to : " + server.toString () );
	}

}
