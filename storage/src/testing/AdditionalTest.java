package testing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;

import org.junit.Test;

import app_kvEcs.ECSCommand;
import app_kvEcs.ECSMessage;
import app_kvServer.ServerMessage;
import client.SerializationUtil;
import common.ServerInfo;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.ClientMessage;
import junit.framework.TestCase;

public class AdditionalTest extends TestCase {

	@Test
	public void testClientNotRespSerialization () {

		ClientMessage message = new ClientMessage ();
		message.setStatus ( StatusType.SERVER_NOT_RESPONSIBLE );
		List < ServerInfo > metaData = new ArrayList < ServerInfo > ();
		ServerInfo s1 = new ServerInfo ( "1222" , 900 , "1" , "10" );
		ServerInfo s2 = new ServerInfo ( "1333" , 880 , "11" , "20" );
		ServerInfo s3 = new ServerInfo ( "3333" , 333 , "33" , "43" );
		ServerInfo s4 = new ServerInfo ( "4444" , 666 , "44" , "55" );
		
		s1.setFirstReplicaInfo(s2);
		s1.setSecondReplicaInfo(s3);
		
		s2.setFirstReplicaInfo(s3);
		s2.setSecondReplicaInfo(s4);
		
		s3.setFirstReplicaInfo(s4);
		s3.setSecondReplicaInfo(s1);
		
		s4.setFirstReplicaInfo(s1);
		s4.setSecondReplicaInfo(s2);
		
		metaData.add ( s1 );
		metaData.add ( s2 );
		metaData.add ( s3 );
		metaData.add ( s4 );
		message.setMetadata ( metaData );

		assertNotNull ( "Message is null" , message );

		byte [] byteStream = SerializationUtil.toByteArray ( message );

		assertNotNull ( "Message serialization failed." + byteStream );

		ClientMessage deserializedMessage;
		try {
			deserializedMessage = ( ClientMessage ) SerializationUtil
					.toObject ( byteStream );
			assertNotNull ( "Message deserialization failed." ,
					deserializedMessage );
			assertTrue ( message.getStatus ().equals (
					deserializedMessage.getStatus () ) );
			assertTrue ( message.getMetadata ().equals (
					deserializedMessage.getMetadata () ) );
		} catch ( UnsupportedDataTypeException e ) {
			System.out.println ( e.getMessage () );

		}
	}

	@Test
	public void testClientServerStoppedSerialization () {

		ClientMessage message = new ClientMessage ();
		message.setStatus ( StatusType.SERVER_STOPPED );
		assertNotNull ( "Message is null" , message );

		byte [] byteStream = SerializationUtil.toByteArray ( message );

		assertNotNull ( "Message serialization failed." + byteStream );

		ClientMessage deserializedMessage;
		try {
			deserializedMessage = ( ClientMessage ) SerializationUtil
					.toObject ( byteStream );
			assertNotNull ( "Message deserialization failed." ,
					deserializedMessage );
			assertTrue ( message.getStatus ().equals (
					deserializedMessage.getStatus () ) );
		} catch ( UnsupportedDataTypeException e ) {
			System.out.println ( e.getMessage () );

		}
	}

	/**
	 * This test id for testing the serialization of <code>KVMessage</code>
	 * using the put function.
	 * 
	 * @result the <code>KVMessage</code> will be generated from the key and
	 *         value and compared with the one generated from the server
	 */
	@Test
	public void testPutSerialization () {

		ClientMessage putMessage = new ClientMessage ();
		putMessage.setKey ( "Key1" );
		putMessage.setValue ( "Value1" );
		putMessage.setStatus ( StatusType.PUT );

		assertNotNull ( "Message is null" , putMessage );

		byte [] byteStream = SerializationUtil.toByteArray ( putMessage );

		assertNotNull ( "Message serialization failed." + byteStream );

		KVMessage deserializedMessage;
		try {
			deserializedMessage = ( KVMessage ) SerializationUtil
					.toObject ( byteStream );
			assertNotNull ( "Message deserialization failed." ,
					deserializedMessage );
			assertTrue ( putMessage.getKey ().equals (
					deserializedMessage.getKey () ) );
			assertTrue ( putMessage.getStatus ().equals (
					deserializedMessage.getStatus () ) );
			assertTrue ( putMessage.getValue ().equals (
					deserializedMessage.getValue () ) );
		} catch ( UnsupportedDataTypeException e ) {

		}
	}

	@Test
	public void testServerMoveDataSerialization () {

		ServerMessage message = new ServerMessage ();

		Map < String , String > data = new HashMap < String , String > ();
		data.put ( "k1" , "v1" );
		data.put ( "k2" , "v2" );
		data.put ( "k3" , "v3" );
		data.put ( "k4" , "v4" );
		data.put ( "k5" , "v5" );
		message.setData ( data );

		assertNotNull ( "Message is null" , message );

		byte [] byteStream = SerializationUtil.toByteArray ( message );

		assertNotNull ( "Message serialization failed." + byteStream );

		ServerMessage deserializedMessage;
		try {
			deserializedMessage = ( ServerMessage ) SerializationUtil
					.toObject ( byteStream );
			assertNotNull ( "Message deserialization failed." ,
					deserializedMessage );
			assertTrue ( message.getData ().equals (
					deserializedMessage.getData () ) );
		} catch ( UnsupportedDataTypeException e ) {
			System.out.println ( e.getMessage () );

		}
	}

	/**
	 * This test is for testing the serialization of <code>ECSMessage</code>
	 * 
	 */
	@Test
	public void testECSInitSerialization () {

		ECSMessage initMessage = new ECSMessage ();
		initMessage.setActionType ( ECSCommand.INIT );
		List < ServerInfo > metaData = new ArrayList < ServerInfo > ();
		ServerInfo s1 = new ServerInfo ( "1222" , 900 , "1" , "10" );
		ServerInfo s2 = new ServerInfo ( "1333" , 880 , "11" , "20" );
		ServerInfo s3 = new ServerInfo ( "3333" , 333 , "33" , "43" );
		ServerInfo s4 = new ServerInfo ( "4444" , 666 , "44" , "55" );
		
		s1.setFirstReplicaInfo(s2);
		s1.setSecondReplicaInfo(s3);
		
		s2.setFirstReplicaInfo(s3);
		s2.setSecondReplicaInfo(s4);
		
		s3.setFirstReplicaInfo(s4);
		s3.setSecondReplicaInfo(s1);
		
		s4.setFirstReplicaInfo(s1);
		s4.setSecondReplicaInfo(s2);
		
		
		metaData.add ( s1 );
		metaData.add ( s2 );
		metaData.add ( s3 );
		metaData.add ( s4 );
		
		initMessage.setMetaData ( metaData );

		assertNotNull ( "Message is null" , initMessage );

		byte [] byteStream = SerializationUtil.toByteArray ( initMessage );

		assertNotNull ( "Message serialization failed." + byteStream );

		ECSMessage deserializedMessage;
		try {
			deserializedMessage = ( ECSMessage ) SerializationUtil
					.toObject ( byteStream );
			assertNotNull ( "Message deserialization failed." ,
					deserializedMessage );
			assertTrue ( initMessage.getActionType ().equals (
					deserializedMessage.getActionType () ) );
			assertTrue ( initMessage.getMetaData ().equals (
					deserializedMessage.getMetaData () ) );
			for(int i=0;i <initMessage.getMetaData().size();i++){
				assertTrue (initMessage.getMetaData().get(i).getFirstReplicaInfo()
						.equals(deserializedMessage.getMetaData().get(i).getFirstReplicaInfo()));				
			}
			
		} catch ( UnsupportedDataTypeException e ) {
			System.out.println ( e.getMessage () );

		}
	}

	/**
	 * This test is for testing the serialization of <code>ECSMessage</code>
	 * 
	 */
	@Test
	public void testECSStartSerialization () {

		ECSMessage message = new ECSMessage ();
		message.setActionType ( ECSCommand.START );

		assertNotNull ( "Message is null" , message );

		byte [] byteStream = SerializationUtil.toByteArray ( message );

		assertNotNull ( "Message serialization failed." + byteStream );

		ECSMessage deserializedMessage;
		try {
			deserializedMessage = ( ECSMessage ) SerializationUtil
					.toObject ( byteStream );
			assertNotNull ( "Message deserialization failed." ,
					deserializedMessage );
			assertTrue ( message.getActionType ().equals (
					deserializedMessage.getActionType () ) );
		} catch ( UnsupportedDataTypeException e ) {

		}
	}

	/**
	 * This test is for testing the serialization of <code>ECSMessage</code>
	 * 
	 */
	@Test
	public void testECSStopSerialization () {

		ECSMessage message = new ECSMessage ();
		message.setActionType ( ECSCommand.STOP );

		assertNotNull ( "Message is null" , message );

		byte [] byteStream = SerializationUtil.toByteArray ( message );

		assertNotNull ( "Message serialization failed." + byteStream );

		ECSMessage deserializedMessage;
		try {
			deserializedMessage = ( ECSMessage ) SerializationUtil
					.toObject ( byteStream );
			assertNotNull ( "Message deserialization failed." ,
					deserializedMessage );
			assertTrue ( message.getActionType ().equals (
					deserializedMessage.getActionType () ) );
		} catch ( UnsupportedDataTypeException e ) {

		}
	}

	/**
	 * This test is for testing the serialization of <code>ECSMessage</code>
	 * 
	 */
	@Test
	public void testECSShutdownSerialization () {

		ECSMessage message = new ECSMessage ();
		message.setActionType ( ECSCommand.SHUT_DOWN );

		assertNotNull ( "Message is null" , message );

		byte [] byteStream = SerializationUtil.toByteArray ( message );

		assertNotNull ( "Message serialization failed." + byteStream );

		ECSMessage deserializedMessage;
		try {
			deserializedMessage = ( ECSMessage ) SerializationUtil
					.toObject ( byteStream );
			assertNotNull ( "Message deserialization failed." ,
					deserializedMessage );
			assertTrue ( message.getActionType ().equals (
					deserializedMessage.getActionType () ) );
		} catch ( UnsupportedDataTypeException e ) {

		}
	}

	/**
	 * This test is for testing the serialization of <code>ECSMessage</code>
	 * 
	 */
	@Test
	public void testECSSetLockSerialization () {

		ECSMessage message = new ECSMessage ();
		message.setActionType ( ECSCommand.SET_WRITE_LOCK );

		assertNotNull ( "Message is null" , message );

		byte [] byteStream = SerializationUtil.toByteArray ( message );

		assertNotNull ( "Message serialization failed." + byteStream );

		ECSMessage deserializedMessage;
		try {
			deserializedMessage = ( ECSMessage ) SerializationUtil
					.toObject ( byteStream );
			assertNotNull ( "Message deserialization failed." ,
					deserializedMessage );
			assertTrue ( message.getActionType ().equals (
					deserializedMessage.getActionType () ) );
		} catch ( UnsupportedDataTypeException e ) {

		}
	}

	@Test
	public void testECSAckSerialization () {

		ECSMessage message = new ECSMessage ();
		message.setActionType ( ECSCommand.ACK );

		assertNotNull ( "Message is null" , message );

		byte [] byteStream = SerializationUtil.toByteArray ( message );

		assertNotNull ( "Message serialization failed." + byteStream );

		ECSMessage deserializedMessage;
		try {
			deserializedMessage = ( ECSMessage ) SerializationUtil
					.toObject ( byteStream );
			assertNotNull ( "Message deserialization failed." ,
					deserializedMessage );
			assertTrue ( message.getActionType ().equals (
					deserializedMessage.getActionType () ) );
		} catch ( UnsupportedDataTypeException e ) {

		}
	}

	/**
	 * This test is for testing the serialization of <code>ECSMessage</code>
	 * 
	 */
	@Test
	public void testECSMoveDataSerialization () {

		ECSMessage message = new ECSMessage ();
		message.setActionType ( ECSCommand.MOVE_DATA );
		message.setMoveFromIndex ( "10" );
		message.setMoveToIndex ( "20" );
		message.setMoveToServer ( new ServerInfo ( "1009" , 666 ) );

		assertNotNull ( "Message is null" , message );

		byte [] byteStream = SerializationUtil.toByteArray ( message );

		assertNotNull ( "Message serialization failed." + byteStream );

		ECSMessage deserializedMessage;
		try {
			deserializedMessage = ( ECSMessage ) SerializationUtil
					.toObject ( byteStream );
			assertNotNull ( "Message deserialization failed." ,
					deserializedMessage );
			assertTrue ( message.getActionType ().equals (
					deserializedMessage.getActionType () ) );
			assertTrue ( message.getMoveFromIndex ().equals (
					deserializedMessage.getMoveFromIndex () ) );
			assertTrue ( message.getMoveToIndex ().equals (
					deserializedMessage.getMoveToIndex () ) );
			assertTrue ( message.getMoveToServer ().equals (
					deserializedMessage.getMoveToServer () ) );
		} catch ( UnsupportedDataTypeException e ) {

		}
	}

	/**
	 * This test id for testing the serialization of <code>KVMessage</code>
	 * using the get function.
	 * 
	 * @result the <code>KVMessage</code> will be generated from the key and
	 *         value and compared with the one generated from the server
	 */
	@Test
	public void testGetSerialization () {

		ClientMessage getMessage = new ClientMessage ();
		getMessage.setKey ( "Key1" );
		getMessage.setValue ( "Value1" );
		getMessage.setStatus ( StatusType.GET );

		assertNotNull ( "Message is null" , getMessage );

		byte [] byteStream = SerializationUtil.toByteArray ( getMessage );

		assertNotNull ( "Message serialization failed." + byteStream );

		KVMessage deserializedMessage;
		try {
			deserializedMessage = ( KVMessage ) SerializationUtil
					.toObject ( byteStream );
			assertNotNull ( "Message deserialization failed." ,
					deserializedMessage );

			assertTrue ( getMessage.getKey ().equals (
					deserializedMessage.getKey () ) );
			assertTrue ( getMessage.getStatus ().equals (
					deserializedMessage.getStatus () ) );
			assertTrue ( getMessage.getValue ().equals (
					deserializedMessage.getValue () ) );

		} catch ( UnsupportedDataTypeException e ) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}

	}

	/**
	 * This test id for testing the serialization of <code>KVMessage</code> enum
	 * types in this test <code>StatusType.GET_ERROR</code>
	 * 
	 * @result the <code>KVMessage</code> will be generated from the key and
	 *         value and compared with the one generated from the server
	 */
	@Test
	public void testGetErrorSerialization () {

		ClientMessage getMessage = new ClientMessage ();
		getMessage.setKey ( "Key1" );
		getMessage.setValue ( "Value1" );
		getMessage.setStatus ( StatusType.GET_ERROR );

		assertNotNull ( "Message is null" , getMessage );

		byte [] byteStream = SerializationUtil.toByteArray ( getMessage );

		assertNotNull ( "Message serialization failed." + byteStream );

		KVMessage deserializedMessage;
		try {
			deserializedMessage = ( KVMessage ) SerializationUtil
					.toObject ( byteStream );
			assertNotNull ( "Message deserialization failed." ,
					deserializedMessage );

			assertTrue ( getMessage.getKey ().equals (
					deserializedMessage.getKey () ) );
			assertTrue ( getMessage.getStatus ().equals (
					deserializedMessage.getStatus () ) );
			assertTrue ( getMessage.getValue ().equals (
					deserializedMessage.getValue () ) );
		} catch ( UnsupportedDataTypeException e ) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}

	}

	/**
	 * This test id for testing the serialization of <code>KVMessage</code> enum
	 * types in this test <code>StatusType.DELETE_SUCCESS</code>
	 * 
	 * @result the <code>KVMessage</code> will be generated from the key and
	 *         value and compared with the one generated from the server
	 */
	@Test
	public void testDeleteSerialization () {

		ClientMessage deleteMessage = new ClientMessage ();
		deleteMessage.setKey ( "Key1" );
		deleteMessage.setValue ( "Value1" );
		deleteMessage.setStatus ( StatusType.DELETE_SUCCESS );

		assertNotNull ( "Message is null" , deleteMessage );

		byte [] byteStream = SerializationUtil.toByteArray ( deleteMessage );

		assertNotNull ( "Message serialization failed." + byteStream );

		KVMessage deserializedMessage;
		try {
			deserializedMessage = ( KVMessage ) SerializationUtil
					.toObject ( byteStream );
			assertNotNull ( "Message deserialization failed." ,
					deserializedMessage );

			assertTrue ( deleteMessage.getValue ().equals (
					deserializedMessage.getValue () ) );
			assertTrue ( deleteMessage.getKey ().equals (
					deserializedMessage.getKey () ) );
			assertTrue ( deleteMessage.getStatus ().equals (
					deserializedMessage.getStatus () ) );

		} catch ( UnsupportedDataTypeException e ) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}

	}

}
