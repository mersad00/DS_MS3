package testing;

import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import client.SerializationUtil;
import common.Hasher;
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
	    
	    KVMessage deserializedMessage = SerializationUtil.toObject(byteStream);
	    
	    assertNotNull("Message deserialization failed.",deserializedMessage);
	    
	    assertTrue(putMessage.getKey().equals(deserializedMessage.getKey()));
	    assertTrue(putMessage.getStatus().equals(deserializedMessage.getStatus()));
	    assertTrue(putMessage.getValue().equals(deserializedMessage.getValue()));
	    
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
	    
	    KVMessage deserializedMessage = SerializationUtil.toObject(byteStream);
	    
	    assertNotNull("Message deserialization failed.",deserializedMessage);
	    
	    assertTrue(getMessage.getKey().equals(deserializedMessage.getKey()));
	    assertTrue(getMessage.getStatus().equals(deserializedMessage.getStatus()));
	    assertTrue(getMessage.getValue().equals(deserializedMessage.getValue()));
	    
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
	    
	    KVMessage deserializedMessage = SerializationUtil.toObject(byteStream);
	    
	    assertNotNull("Message deserialization failed.",deserializedMessage);

	    assertTrue(getMessage.getKey().equals(deserializedMessage.getKey()));
	    assertTrue(getMessage.getStatus().equals(deserializedMessage.getStatus()));
	    assertTrue(getMessage.getValue().equals(deserializedMessage.getValue()));
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
	    
	    KVMessage deserializedMessage = SerializationUtil.toObject(byteStream);
	    
	    assertNotNull("Message deserialization failed.",deserializedMessage);
	    
	    
	    assertTrue(deleteMessage.getValue().equals(deserializedMessage.getValue()));
	    assertTrue(deleteMessage.getKey().equals(deserializedMessage.getKey()));
	    assertTrue(deleteMessage.getStatus().equals(deserializedMessage.getStatus()));

	    
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
