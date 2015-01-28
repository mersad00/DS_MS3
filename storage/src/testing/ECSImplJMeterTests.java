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

public class ECSImplJMeterTests extends TestCase {
    static {
	try {
	    new LogSetup("test.log", Level.ALL);
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }
    
    public ECSImplJMeterTests(String threadName) {
    	this.threadName = threadName;
        }
    
    /*public ECSImplJMeterTests() {
    	//this.threadName = Thread.currentThread().getName();
        }*/
    private String threadName;

    @Test
	public void testPutJmeterBastard() {
		// create new client
		ServerInfo firstServer = getServerInfo(0);
		KVStore kvClient = new KVStore(firstServer.getAddress(),
				firstServer.getPort());
		try {
			kvClient.connect();
			KVMessage response = null;
			String randomKey = Dictionary.getRandomKey();
			response = kvClient.put(randomKey, Dictionary.getValue(randomKey));
			if (response.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE)) {
				// retry
				kvClient.updateMetadata(((ClientMessage) response)
						.getMetadata());
				
				response = kvClient.put(randomKey, Dictionary.getValue(randomKey));
				assertFalse(response.getStatus() == StatusType.PUT_ERROR);
			}

		} catch (Exception e) {
		
		}
		kvClient.disconnect();
	}

	@Test
	public void testGetJmeterBastard() {
		// create new client
		ServerInfo firstServer = getServerInfo(0);
		KVStore kvClient = new KVStore(firstServer.getAddress(),
				firstServer.getPort());
		try {
			kvClient.connect();
			KVMessage response = null;
			String randomKey = Dictionary.getRandomKey();
			response = kvClient.get(randomKey);
			if (response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
				// retry
				kvClient.updateMetadata(((ClientMessage) response)
						.getMetadata());
				response = kvClient.get(randomKey);
				assertTrue(response.getStatus() == StatusType.GET_SUCCESS);
				// assertTrue(threadName.equals(response.getValue()));
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
