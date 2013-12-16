package testing;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import threads.ServerThread;

import client.SerializationUtil;

import common.ServerInfo;
import common.messages.AbstractMessage;

import app_kvEcs.ECSCommand;
import app_kvEcs.ECSMessage;
import app_kvServer.KVServer;
import app_kvServer.ServerMessage;

public class ServerEcsCommunicationTests {

	private KVServer firstServer;
	private KVServer secondServer;
	private KVServer thirdServer;

	private Socket connectionToOtherServer = null;
	private OutputStream output = null;
	private InputStream input = null;
	
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	@Before
	public void setUp () {
		firstServer = new KVServer ( 50000 );
		secondServer = new KVServer ( 50001 );
		thirdServer = new KVServer ( 50002 );
		
	}

	@Test
	public void testECSCommunicationFlow () throws IOException {
		ServerThread server1 = new ServerThread ( firstServer );
		ServerThread server2 = new ServerThread ( secondServer );
		server1.start ();
		server2.start ();

		ECSMessage initMessage = new ECSMessage ();
		initMessage.setActionType ( ECSCommand.INIT );
		initMessage.setMetaData ( getRandomMetadata () );

		ECSMessage startMessage = new ECSMessage ();
		startMessage.setActionType ( ECSCommand.START );

		ECSMessage stopMessage = new ECSMessage ();
		stopMessage.setActionType ( ECSCommand.STOP );
		
		
		ECSMessage shutdownMessage = new ECSMessage ();
		shutdownMessage.setActionType ( ECSCommand.SHUT_DOWN );
		
		ECSMessage writeLockMessage = new ECSMessage ();
		writeLockMessage.setActionType ( ECSCommand.SET_WRITE_LOCK );
		
		ECSMessage releaseLockMessage = new ECSMessage ();
		releaseLockMessage.setActionType ( ECSCommand.RELEASE_LOCK );
		
		
		ECSMessage sendDataMessage = new ECSMessage ();
		sendDataMessage.setActionType ( ECSCommand.MOVE_DATA );
		sendDataMessage.setMoveFromIndex ( "C2ADD694BF942DC77B376592D9C862CD" );
		sendDataMessage.setMoveToIndex ( "78F825AAA0103319AAA1A30BF4FE3ADA" );
		sendDataMessage.setMoveToServer ( new ServerInfo ("localhost", 50001) );
		
		ECSMessage updateMessage = new ECSMessage ();
		updateMessage.setActionType ( ECSCommand.SEND_METADATA );
		updateMessage.setMetaData ( this.getRandomMetadata () );
		
		initConnection (  new ServerInfo ( "localhost" ,
				50000 ) );
		
		sendMessageToServer ( initMessage );
		sendMessageToServer ( startMessage );
		
		sendMessageToServer ( stopMessage );
		
		sendMessageToServer ( writeLockMessage);
		
		sendMessageToServer ( sendDataMessage );
		ECSMessage ackMsg = receiveMessage();
		assertEquals ( ECSCommand.ACK , ackMsg.getActionType () );
		
		sendMessageToServer ( releaseLockMessage);
		//sendMessageToServer ( shutdownMessage ); //TODO search for other solutions to exit safely
		
		sendMessageToServer ( updateMessage);
		
		ServerSocket server = new ServerSocket (40000); // some blocking operation to test client
		server.accept ();
	}

	private void initConnection ( ServerInfo server ) {
		try {
			connectionToOtherServer = new Socket ( server.getAddress () ,
					server.getPort () );	
			System.out.println(connectionToOtherServer.isConnected ());
			output = connectionToOtherServer.getOutputStream ();
		} catch ( UnknownHostException e ) {
			e.printStackTrace ();
		} catch ( IOException e ) {
			e.printStackTrace ();
		}
	}

	private void sendMessageToServer ( ECSMessage msg ) {
		byte [] msgBytes = SerializationUtil.toByteArray ( msg );
		try {
			output.write ( msgBytes , 0 , msgBytes.length );
			output.flush ();
			
			System.out.println ("sending message");
		} catch ( IOException e ) {
			e.printStackTrace ();
		}

	}

	private List < ServerInfo > getRandomMetadata () {
		List < ServerInfo > metadata = new ArrayList < ServerInfo > ();
		ServerInfo server1 = new ServerInfo ();
		server1.setAddress ( "localhost" );
		server1.setPort ( 50000 );
		server1.setFromIndex ( "1" );
		server1.setToIndex ( "10" );
		metadata.add ( server1 );
		return metadata;
	}

	
	
	private ECSMessage receiveMessage () throws IOException {
		this.input = connectionToOtherServer.getInputStream ();
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
		ECSMessage msg = (ECSMessage)SerializationUtil.toObject ( msgBytes );		
		return msg;
	}

}
