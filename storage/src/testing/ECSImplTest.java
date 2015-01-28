package testing;

import java.io.IOException;

import junit.framework.TestCase;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.junit.Test;

import common.ServerInfo;
import common.messages.ClientMessage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import client.KVStore;
import static org.junit.Assert.*;

public class ECSImplTest extends TestCase {
    static {
	try {
	    new LogSetup("logs/testing/test.log", Level.ALL);
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }
    public void setUp() {
    }

    public void tearDown() {
    }
    
    
    public ECSImplTest(String threadName) {
 	this.threadName = threadName;
     }
   public ECSImplTest() {
     	//this.threadName =  Long.toString(Thread.currentThread().getId());
         }
    
     private String threadName;

    @Test
    public void testEndToEndFlowNewUser() {
	// create new client
	ServerInfo firstServer = getServerInfo(0);
	KVStore kvClient = new KVStore(firstServer.getAddress(),
		firstServer.getPort());
	try {
	    kvClient.connect();
	    String key = threadName ;
	    KVMessage response = null;
	    Exception ex = null;
	    response = kvClient.put(key, key);

	    if (response.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE)) {
		// retry
		kvClient.updateMetadata(((ClientMessage)response).getMetadata());
	
		response = kvClient.put(key, key);
		assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
	    }

	} catch (Exception e) {
	}
	kvClient.disconnect();

    }

    private static ServerInfo getServerInfo(int i) {
	ServerInfo serverInfo = new ServerInfo("localhost",50010);
	return serverInfo;
    }

}
