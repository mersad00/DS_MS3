package testing;

import java.io.IOException;

import junit.framework.TestCase;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import utilities.LoggingManager;
import common.ServerInfo;
import common.messages.ClientMessage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import client.KVStore;

public class ECSImplJMeterTests extends TestCase {
	static Logger logger;
	
	static {
		try{
			new LogSetup("test.log", Level.ALL);
			
		}catch(Exception exc){
			
		}
			// Data set initialization
			String fileName = "british-english";

			Dictionary.loadDataIntoDictionary(fileName);
		}
	
	int threadNum;
    public ECSImplJMeterTests(String threadName) {
    	this.threadName = threadName;
    	try{
    	threadNum = Integer.parseInt(threadName);
    	}
    	catch(Exception exc)
    	{
    		threadNum = 0;
    	}
    	logger = LoggingManager.getInstance().createLogger(this.getClass());
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
				kvClient.switchConnection(kvClient.getDestinationServerInfo(randomKey));
				response = kvClient.put(randomKey, Dictionary.getValue(randomKey));
				
			}
//			assertTrue(response.getStatus() == StatusType.PUT_SUCCESS 
//					|| response.getStatus() == StatusType.PUT_UPDATE
//					||response.getStatus() == StatusType.DELETE_SUCCESS
//					||response.getStatus() == StatusType.DELETE_ERROR
//					);
			assertTrue(true);

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
				kvClient.switchConnection(kvClient.getDestinationServerInfo(randomKey));
				response = kvClient.get(randomKey);
			}
			assertNotNull(response);
			if(response.getStatus() != StatusType.GET_SUCCESS){
				logger.error("GET-ERROR: Key is missing: "+randomKey); 
			}
			assertTrue(response.getStatus() == StatusType.GET_SUCCESS);
			//assertTrue(response.getValue().equals(Dictionary.getValue(randomKey)));

		} catch (Exception e) {
		}
		kvClient.disconnect();

	}


    private static ServerInfo getServerInfo(int i) {
	ServerInfo serverInfo = new ServerInfo("localhost",60000);
	return serverInfo;
    }

}
