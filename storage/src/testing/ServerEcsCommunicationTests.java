package testing;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import threads.ServerThread;

import client.SerializationUtil;

import common.Hasher;
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
	
	private Hasher md5Hasher;
	
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	@Before
	public void setUp () {
		firstServer = new KVServer ( 50000 );
		secondServer = new KVServer ( 50001 );
		thirdServer = new KVServer ( 50002 );
		this.md5Hasher = new Hasher();
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
		
		ServerInfo serverInfo1 = new ServerInfo ( "localhost" , 50000 );
		ServerInfo serverInfo2 = new ServerInfo ( "localhost" , 50001 );
		
		sendMessageToServer ( initMessage , serverInfo1 );
		sendMessageToServer ( startMessage , serverInfo1 );
		
		sendMessageToServer ( initMessage , serverInfo2 );
		sendMessageToServer ( startMessage , serverInfo2 );
		
		//sendMessageToServer ( stopMessage );
		
		//sendMessageToServer ( writeLockMessage);
		
		//sendMessageToServer ( sendDataMessage );
		//ECSMessage ackMsg = receiveMessage();
		//assertEquals ( ECSCommand.ACK , ackMsg.getActionType () );
		
		//sendMessageToServer ( releaseLockMessage);
		//sendMessageToServer ( shutdownMessage ); //TODO search for other solutions to exit safely, 
		
		//sendMessageToServer ( updateMessage);
		
		ServerSocket server = new ServerSocket (40000); // some blocking operation to test client
		server.accept ();
	}


	private void sendMessageToServer ( ECSMessage msg , ServerInfo server) {
		byte [] msgBytes = SerializationUtil.toByteArray ( msg );
		try {
			connectionToOtherServer = new Socket ( server.getAddress () ,
					server.getPort () );				
			output = connectionToOtherServer.getOutputStream ();
			output.write ( msgBytes , 0 , msgBytes.length );
			output.flush ();						
		} catch ( IOException e ) {
			e.printStackTrace ();
		}

	}

	private List < ServerInfo > getRandomMetadata () {
		List < ServerInfo > metadata = new ArrayList < ServerInfo > ();
		ServerInfo server1 = new ServerInfo ();
		server1.setAddress ( "localhost" );
		server1.setPort ( 50000 );
		
		ServerInfo server2 = new ServerInfo ();
		server2.setAddress ( "localhost" );
		server2.setPort ( 50001 );
		
		metadata.add ( server1 );
		metadata.add ( server2 );
		metadata = this.calculateMetaData ( metadata );
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

	private List<ServerInfo> calculateMetaData(List<ServerInfo> serversToStart) {
		
		for (ServerInfo server: serversToStart){// loop and the hash values
		    String hashKey = md5Hasher.getHash(server.getAddress()+":"+server.getPort());
		    //the to index is the value of the server hash
		    server.setToIndex(hashKey);
		}
		
		Collections.sort(serversToStart, new Comparator<ServerInfo>() { // sort the list by the hashes
		    @Override
		    public int compare(ServerInfo o1, ServerInfo o2) {
		         return md5Hasher.compareHashes(o1.getToIndex() , o2.getToIndex());
		         }
		});			
		    
		for (int i = 0; i < serversToStart.size(); i++) {
		    ServerInfo server = serversToStart.get(i);
		    ServerInfo predecessor;
		    if(i==0){// first node is a special case.
			predecessor = serversToStart.get(serversToStart.size()-1);
		    }else{
			predecessor = serversToStart.get(i-1);
		    }
		    server.setFromIndex(predecessor.getToIndex());
		    
		}		
		return serversToStart;
		}
}
