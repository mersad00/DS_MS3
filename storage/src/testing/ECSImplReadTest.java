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

public class ECSImplReadTest {
    static {
	try {
	    new LogSetup("logs/testing/test.log", Level.ALL);
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }
    

    public ECSImplReadTest(String threadName) {
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
	    response = kvClient.get(key);
//	    assertTrue(
//		    ex.getMessage() + response.getMessageType(),
//		    ex == null
//			    && (response.getStatus() == StatusType.GET_SUCCESS
//				    || response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE || response
//				    .getStatus() == StatusType.SERVER_STOPPED));
	    if (response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
		// retry
		kvClient.updateMetadata(((ClientMessage)response).getMetadata());
		response = kvClient.get(key);
		assertTrue(response.getStatus() == StatusType.GET_SUCCESS);
		assertTrue(threadName.equals(response.getValue()));
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
