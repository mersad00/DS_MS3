package testing;

import java.io.IOException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.junit.Test;

import common.ServerInfo;
import common.messages.ClientMessage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import client.KVStore;
import static org.junit.Assert.*;

public class ECSImplTest {
    static {
	try {
	    new LogSetup("logs/testing/test.log", Level.ALL);
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    
    public ECSImplTest(String threadName) {
 	this.threadName = threadName;
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
//	    assertTrue(ex == null
//			    && (response.getStatus() == StatusType.PUT_SUCCESS
//				    || response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE || response
//				    .getStatus() == StatusType.SERVER_STOPPED));
//	
	 
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
	ServerInfo serverInfo = new ServerInfo("localhost",6000);
	return serverInfo;
    }

}
