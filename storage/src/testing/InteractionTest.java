package testing;

import java.io.IOException;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import app_kvServer.KVServer;

import client.KVStore;
import junit.framework.TestCase;
import common.Hasher;
import common.ServerInfo;
import common.messages.ClientMessage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;


public class InteractionTest extends TestCase {

	private KVStore kvClient;
	private KVServer server;
	private List<ServerInfo> metadata;
	
	
	@BeforeClass
	public void setUp() throws IOException {
		kvClient = new KVStore("localhost", 50000);				
		try {
			kvClient.connect();			
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}
	

	@Test
    public void testPut() {
        String key = "foobar";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
            if(response.getStatus ().equals (StatusType.SERVER_NOT_RESPONSIBLE )){
            	metadata = ((ClientMessage)response).getMetadata ();
            	kvClient = new KVStore ( this.getDestinationServer ( metadata , key ) );
            	kvClient.connect ();
            	response = kvClient.put(key, value);
            }
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
    }
	

	@Test
	public void testPutDisconnected() {
		kvClient.disconnect();
		String key = "foo";
		String value = "bar";
		Exception ex = null;

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, initialValue);
			if(response.getStatus ().equals (StatusType.SERVER_NOT_RESPONSIBLE )){
            	metadata = ((ClientMessage)response).getMetadata ();
            	kvClient = new KVStore ( this.getDestinationServer ( metadata , key ) );
            	kvClient.connect ();
            	response = kvClient.put(key, initialValue);
            }
			response = kvClient.put(key, updatedValue);
			
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}
	
	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");
			if(response.getStatus ().equals (StatusType.SERVER_NOT_RESPONSIBLE )){
            	metadata = ((ClientMessage)response).getMetadata ();
            	kvClient = new KVStore ( this.getDestinationServer ( metadata , key ) );
            	kvClient.connect ();
            	response = kvClient.put(key, value);
            }
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}
	
	@Test
	public void testGet() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

			try {
				kvClient.get ( key );
				if(response.getStatus ().equals (StatusType.SERVER_NOT_RESPONSIBLE )){
	            	metadata = ((ClientMessage)response).getMetadata ();
	            	kvClient = new KVStore ( this.getDestinationServer ( metadata , key ) );
	            	kvClient.connect ();
	            	response = kvClient.put(key, value);
	            }
				response = kvClient.get(key);
			} catch (Exception e) {
				ex = e;
			}
		
		assertTrue(ex == null && response.getValue().equals("bar"));
	}

	@Test
	public void testGetUnsetValue() {
		String key = "an unset value";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
			if(response.getStatus ().equals (StatusType.SERVER_NOT_RESPONSIBLE )){
            	metadata = ((ClientMessage)response).getMetadata ();
            	kvClient = new KVStore ( this.getDestinationServer ( metadata , key ) );
            	kvClient.connect ();
            	response = kvClient.get(key);
            }
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}
	
	private ServerInfo getDestinationServer (List<ServerInfo> servers, String key){
		ServerInfo destination = null;
			Hasher hasher = new Hasher();
				for(ServerInfo server: metadata){
					if( hasher.isInRange ( server.getFromIndex () , server.getToIndex () , hasher.getHash ( key ) )){
						return server;
					}
						
				}
		return destination;
	}


}
