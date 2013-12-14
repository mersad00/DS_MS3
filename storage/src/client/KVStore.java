package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.Hasher;
import common.ServerInfo;
import common.messages.KVMessage;
import common.messages.ClientMessage;

public class KVStore implements KVCommInterface {

	private Logger logger = Logger.getRootLogger ();
	private ServerInfo currentDestinationServer;

	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	private List < ServerInfo > metadata;
	private KVMessage lastSentMessage;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address
	 *            the address of the KVServer
	 * @param port
	 *            the port of the KVServer
	 */
	public KVStore ( String address , int port ) {
		this ( new ServerInfo ( address , port ) );
	}

	public KVStore ( ServerInfo serverInfo ) {
		this.currentDestinationServer = serverInfo;
		this.metadata = new ArrayList < ServerInfo > ();
	}

	@Override
	public void connect () throws IOException {
		try {
			clientSocket = new Socket ( currentDestinationServer.getAddress () ,
					currentDestinationServer.getPort () );
			output = clientSocket.getOutputStream ();
			input = clientSocket.getInputStream ();
			logger.info ( "Connection established with server" );
		} catch ( IOException ioe ) {

			logger.error ( "Connection could not be established!" );
			throw ioe;
		}
	}

	public void switchConnection ( ServerInfo newDestinationServerInfo )
			throws IOException {
		this.tearDownConnection ();
		this.currentDestinationServer = newDestinationServerInfo;
		this.connect ();

		logger.info ( "switch connection to server : "
				+ newDestinationServerInfo.getAddress () + " on port : "
				+ newDestinationServerInfo.getPort () );
	}

	@Override
	public void disconnect () {
		try {
			tearDownConnection ();
		} catch ( IOException ioe ) {
			logger.error ( "Unable to close connection!" );
		}
	}

	private void tearDownConnection () throws IOException {
		logger.info ( "tearing down the connection ..." );
		if ( clientSocket != null ) {
			input.close ();
			output.close ();
			clientSocket.close ();
			clientSocket = null;

			logger.info ( "connection closed!" );
		}
	}

	@Override
	public KVMessage put ( String key , String value ) throws IOException {
		ClientMessage msg = new ClientMessage ();
		KVMessage receivedMsg = null;
		msg.setKey ( key );
		msg.setValue ( value );
		msg.setStatus ( KVMessage.StatusType.PUT );
		ServerInfo tempInfo = this.getDestinationServerInfo ( key );
		try {
			if ( ! this.currentDestinationServer.equals ( tempInfo ) ) {
				this.switchConnection ( tempInfo );
			}
			this.sendMessage ( msg );
			receivedMsg = this.receiveMessage ();

		} catch ( IOException e ) {
			logger.error ( "error in sending or receiving message" );
			throw e;
		}
		return receivedMsg;
	}

	@Override
	public KVMessage get ( String key ) throws IOException {
		ClientMessage msg = new ClientMessage ();
		KVMessage receivedMsg = null;
		msg.setKey ( key );
		msg.setStatus ( KVMessage.StatusType.GET );
		ServerInfo tempInfo = this.getDestinationServerInfo ( key );
		try {
			if ( ! this.currentDestinationServer.equals ( tempInfo ) ) {
				this.switchConnection ( tempInfo );
			}
			this.sendMessage ( msg );
			receivedMsg = this.receiveMessage ();

		} catch ( IOException e ) {
			logger.error ( "error in sending or receiving message" );
		}
		return receivedMsg;
	}

	private KVMessage receiveMessage () throws IOException {
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
		KVMessage msg = ( KVMessage ) SerializationUtil.toObject ( msgBytes );
		logger.info ( "Receive message:\t '" + msg.getKey () + "'" );
		return msg;
	}

	private void sendMessage ( KVMessage msg ) throws IOException {
		this.lastSentMessage = msg;
		byte [] msgBytes = SerializationUtil.toByteArray ( msg );
		output.write ( msgBytes , 0 , msgBytes.length );
		output.flush ();
		logger.info ( "Send message :\t '" + msg.getKey () + "'" );
	}

	public void updateMetadata ( List < ServerInfo > metadata ) {
		this.metadata = metadata;
		logger.info ( "update metadata with " + metadata.size () + "keys" );
	}

	public KVMessage getLastSentMessage () {
		return this.lastSentMessage;
	}

	private ServerInfo getDestinationServerInfo ( String key ) {
		ServerInfo destinationServer = null;
		// TODO here after amjad upload the new ServerInfo

		return destinationServer;
	}

}
