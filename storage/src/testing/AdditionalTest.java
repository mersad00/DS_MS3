package testing;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.activation.UnsupportedDataTypeException;

import org.junit.Test;

import app_kvEcs.ECSCommand;
import app_kvEcs.ECSMessage;
import client.SerializationUtil;
import common.Hasher;
import common.ServerInfo;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.ClientMessage;
import junit.framework.TestCase;

public class AdditionalTest extends TestCase {


    @Test
    public void testStub() {
	assertTrue(true);
    }


    /**
     * This test id for testing the serialization of <code>KVMessage</code> 
     * using the put function.
     * 
     * @result the <code>KVMessage</code> will be generated from the key and value
     * and compared with the one generated from the server
     */
    @Test
    public void testPutSerialization(){

	ClientMessage putMessage = new ClientMessage();
	putMessage.setKey("Key1");
	putMessage.setValue("Value1");
	putMessage.setStatus(StatusType.PUT);

	assertNotNull("Message is null",putMessage);

	byte[] byteStream = SerializationUtil.toByteArray(putMessage);

	assertNotNull("Message serialization failed."+byteStream);

	KVMessage deserializedMessage;
	try {
	    deserializedMessage = (KVMessage)SerializationUtil.toObject(byteStream);
	    assertNotNull("Message deserialization failed.",deserializedMessage);
	    System.out.println(putMessage.getKey()+"--"+(deserializedMessage.getKey()));
	    assertTrue(putMessage.getKey().equals(deserializedMessage.getKey()));
	    assertTrue(putMessage.getStatus().equals(deserializedMessage.getStatus()));
	    assertTrue(putMessage.getValue().equals(deserializedMessage.getValue()));
	} catch (UnsupportedDataTypeException e) {


	}
    }
    
    /**
     * This test is for testing the serialization of <code>ECSMessage</code> 
     * 
     */
    @Test
    public void testECSInitSerialization(){

	ECSMessage initMessage = new ECSMessage();
	initMessage.setActionType(ECSCommand.INIT);
	List<ServerInfo> metaData = new ArrayList<ServerInfo>();
	ServerInfo s1 = new ServerInfo("1222",900,"1","10");
	metaData.add(s1);
	ServerInfo s2 = new ServerInfo("1333",880,"11","20");
	metaData.add(s2);
	ServerInfo s3 = new ServerInfo("3333",333,"33","43");
	metaData.add(s3);
	ServerInfo s4 = new ServerInfo("4444",666,"44","55");
	metaData.add(s4);
	initMessage.setMetaData(metaData);
	
	assertNotNull("Message is null",initMessage);

	byte[] byteStream = SerializationUtil.toByteArray(initMessage);

	assertNotNull("Message serialization failed."+byteStream);

	ECSMessage deserializedMessage;
	try {
	    deserializedMessage = (ECSMessage)SerializationUtil.toObject(byteStream);
	    assertNotNull("Message deserialization failed.",deserializedMessage);
	    assertTrue(initMessage.getActionType().equals(deserializedMessage.getActionType()));
	    assertTrue(initMessage.getMetaData().equals(deserializedMessage.getMetaData()));
	} catch (UnsupportedDataTypeException e) {
	    System.out.println(e.getMessage());

	}
    }
    
    /**
     * This test is for testing the serialization of <code>ECSMessage</code> 
     * 
     */
    @Test
    public void testECSStartSerialization(){

	ECSMessage message = new ECSMessage();
	message.setActionType(ECSCommand.START);
	
	
	assertNotNull("Message is null",message);

	byte[] byteStream = SerializationUtil.toByteArray(message);

	assertNotNull("Message serialization failed."+byteStream);

	ECSMessage deserializedMessage;
	try {
	    deserializedMessage = (ECSMessage)SerializationUtil.toObject(byteStream);
	    assertNotNull("Message deserialization failed.",deserializedMessage);
	    assertTrue(message.getActionType().equals(deserializedMessage.getActionType()));
	} catch (UnsupportedDataTypeException e) {

	}
    }
    
    
    /**
     * This test is for testing the serialization of <code>ECSMessage</code> 
     * 
     */
    @Test
    public void testECSStopSerialization(){

	ECSMessage message = new ECSMessage();
	message.setActionType(ECSCommand.STOP);
	
	
	assertNotNull("Message is null",message);

	byte[] byteStream = SerializationUtil.toByteArray(message);

	assertNotNull("Message serialization failed."+byteStream);

	ECSMessage deserializedMessage;
	try {
	    deserializedMessage = (ECSMessage)SerializationUtil.toObject(byteStream);
	    assertNotNull("Message deserialization failed.",deserializedMessage);
	    assertTrue(message.getActionType().equals(deserializedMessage.getActionType()));
	} catch (UnsupportedDataTypeException e) {

	}
    }
    
    /**
     * This test is for testing the serialization of <code>ECSMessage</code> 
     * 
     */
    @Test
    public void testECSShutdownSerialization(){

	ECSMessage message = new ECSMessage();
	message.setActionType(ECSCommand.SHUT_DOWN);
	
	
	assertNotNull("Message is null",message);

	byte[] byteStream = SerializationUtil.toByteArray(message);

	assertNotNull("Message serialization failed."+byteStream);

	ECSMessage deserializedMessage;
	try {
	    deserializedMessage = (ECSMessage)SerializationUtil.toObject(byteStream);
	    assertNotNull("Message deserialization failed.",deserializedMessage);
	    assertTrue(message.getActionType().equals(deserializedMessage.getActionType()));
	} catch (UnsupportedDataTypeException e) {

	}
    }
    
    /**
     * This test is for testing the serialization of <code>ECSMessage</code> 
     * 
     */
    @Test
    public void testECSSetLockSerialization(){

	ECSMessage message = new ECSMessage();
	message.setActionType(ECSCommand.SET_WRITE_LOCK);
	
	
	assertNotNull("Message is null",message);

	byte[] byteStream = SerializationUtil.toByteArray(message);

	assertNotNull("Message serialization failed."+byteStream);

	ECSMessage deserializedMessage;
	try {
	    deserializedMessage = (ECSMessage)SerializationUtil.toObject(byteStream);
	    assertNotNull("Message deserialization failed.",deserializedMessage);
	    assertTrue(message.getActionType().equals(deserializedMessage.getActionType()));
	} catch (UnsupportedDataTypeException e) {

	}
    }
    
    
    
    @Test
    public void testECSAckSerialization(){

	ECSMessage message = new ECSMessage();
	message.setActionType(ECSCommand.ACK);
	
	
	assertNotNull("Message is null",message);

	byte[] byteStream = SerializationUtil.toByteArray(message);

	assertNotNull("Message serialization failed."+byteStream);

	ECSMessage deserializedMessage;
	try {
	    deserializedMessage = (ECSMessage)SerializationUtil.toObject(byteStream);
	    assertNotNull("Message deserialization failed.",deserializedMessage);
	    assertTrue(message.getActionType().equals(deserializedMessage.getActionType()));
	} catch (UnsupportedDataTypeException e) {

	}
    }
    /**
     * This test is for testing the serialization of <code>ECSMessage</code> 
     * 
     */
    @Test
    public void testECSMoveDataSerialization(){

	ECSMessage message = new ECSMessage();
	message.setActionType(ECSCommand.MOVE_DATA);
	message.setMoveFromIndex("10");
	message.setMoveToIndex("20");
	message.setMoveToServer(new ServerInfo("1009",666));
	
	assertNotNull("Message is null",message);

	byte[] byteStream = SerializationUtil.toByteArray(message);

	assertNotNull("Message serialization failed."+byteStream);

	ECSMessage deserializedMessage;
	try {
	    deserializedMessage = (ECSMessage)SerializationUtil.toObject(byteStream);
	    assertNotNull("Message deserialization failed.",deserializedMessage);
	    assertTrue(message.getActionType().equals(deserializedMessage.getActionType()));
	    assertTrue(message.getMoveFromIndex().equals(deserializedMessage.getMoveFromIndex())); 
	    assertTrue(message.getMoveToIndex().equals(deserializedMessage.getMoveToIndex())); 
	    assertTrue(message.getMoveToServer().equals(deserializedMessage.getMoveToServer())); 
	} catch (UnsupportedDataTypeException e) {

	}
    }



    /**
     * This test id for testing the serialization of <code>KVMessage</code>
     * using the get function.
     * 
     * @result the <code>KVMessage</code> will be generated from the key and value
     * and compared with the one generated from the server
     */
    @Test
    public void testGetSerialization(){

	ClientMessage getMessage = new ClientMessage();
	getMessage.setKey("Key1");
	getMessage.setValue("Value1");
	getMessage.setStatus(StatusType.GET);

	assertNotNull("Message is null",getMessage);

	byte[] byteStream = SerializationUtil.toByteArray(getMessage);

	assertNotNull("Message serialization failed."+byteStream);

	KVMessage deserializedMessage;
	try {
	    deserializedMessage = (KVMessage)SerializationUtil.toObject(byteStream);
	    assertNotNull("Message deserialization failed.",deserializedMessage);

	    assertTrue(getMessage.getKey().equals(deserializedMessage.getKey()));
	    assertTrue(getMessage.getStatus().equals(deserializedMessage.getStatus()));
	    assertTrue(getMessage.getValue().equals(deserializedMessage.getValue()));

	} catch (UnsupportedDataTypeException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}


    }

    /**
     * This test id for testing the serialization of <code>KVMessage</code> enum types 
     * in this test <code>StatusType.GET_ERROR</code>
     * 
     * @result the <code>KVMessage</code> will be generated from the key and value
     * and compared with the one generated from the server
     */
    @Test
    public void testGetErrorSerialization(){

	ClientMessage getMessage = new ClientMessage();
	getMessage.setKey("Key1");
	getMessage.setValue("Value1");
	getMessage.setStatus(StatusType.GET_ERROR);

	assertNotNull("Message is null",getMessage);

	byte[] byteStream = SerializationUtil.toByteArray(getMessage);

	assertNotNull("Message serialization failed."+byteStream);

	KVMessage deserializedMessage;
	try {
	    deserializedMessage = (KVMessage)SerializationUtil.toObject(byteStream);
	    assertNotNull("Message deserialization failed.",deserializedMessage);

	    assertTrue(getMessage.getKey().equals(deserializedMessage.getKey()));
	    assertTrue(getMessage.getStatus().equals(deserializedMessage.getStatus()));
	    assertTrue(getMessage.getValue().equals(deserializedMessage.getValue()));
	} catch (UnsupportedDataTypeException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}


    }

    /**
     * This test id for testing the serialization of <code>KVMessage</code> enum types 
     * in this test <code>StatusType.DELETE_SUCCESS</code>
     * 
     * @result the <code>KVMessage</code> will be generated from the key and value
     * and compared with the one generated from the server
     */
    @Test
    public void testDeleteSerialization(){

	ClientMessage deleteMessage = new ClientMessage();
	deleteMessage.setKey("Key1");
	deleteMessage.setValue("Value1");
	deleteMessage.setStatus(StatusType.DELETE_SUCCESS);

	assertNotNull("Message is null",deleteMessage);

	byte[] byteStream = SerializationUtil.toByteArray(deleteMessage);

	assertNotNull("Message serialization failed."+byteStream);

	KVMessage deserializedMessage;
	try {
	    deserializedMessage = (KVMessage)SerializationUtil.toObject(byteStream);
	    assertNotNull("Message deserialization failed.",deserializedMessage);


	    assertTrue(deleteMessage.getValue().equals(deserializedMessage.getValue()));
	    assertTrue(deleteMessage.getKey().equals(deserializedMessage.getKey()));
	    assertTrue(deleteMessage.getStatus().equals(deserializedMessage.getStatus()));

	} catch (UnsupportedDataTypeException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}



    }

    @Test
    public void testGetAppropirateServerHash() throws NoSuchAlgorithmException{
	Hasher hasher = new Hasher();
	Set<String> serverHashes = new HashSet<String> ();
	serverHashes.add ( "10" );
	serverHashes.add ( "20" );
	serverHashes.add ( "30" );
	serverHashes.add ( "40" );
	serverHashes.add ( "50" );

	assertEquals ("error 1", "10" , hasher.getAppropriateStorageServerHash ( "5" , serverHashes ) );
	assertEquals ("error 2", "10" , hasher.getAppropriateStorageServerHash ( "60" , serverHashes ) );
	assertEquals ( "40" , hasher.getAppropriateStorageServerHash ( "35" , serverHashes ) );
	assertEquals ( "40" , hasher.getAppropriateStorageServerHash ( "40" , serverHashes ) );
    }


}
