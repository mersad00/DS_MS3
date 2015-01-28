package testing;

import java.io.IOException;

import junit.framework.TestCase;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.junit.Test;

import common.ServerInfo;
import app_kvEcs.ECSMock;

public class EnvJMeterSetupTest extends TestCase {
	private static ECSMock ecsService;

	private int serverNumber;

	static {
		try {
			new LogSetup("test.log", Level.ALL);
			
			// Data set initialization
			String fileName = "british-english";
			Dictionary.loadDataIntoDictionary(fileName);
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

//	public EnvJMeterSetupTest() {
//
//	}

	public EnvJMeterSetupTest(String serverNumber) {
		this.serverNumber = Integer.parseInt(serverNumber);
	}

	public void setUp() {
	}

	public void tearDown() {
	}

	@Test
	public void testOneTimeSetup(){

		try{
		// /ECS initialization
		System.out.println("--in set up " + serverNumber);
		ecsService = new ECSMock(1, "ecs-test.config");
		assertTrue(ecsService.getECS().getActiveServers().size() + "",
				!ecsService.getECS().getActiveServers().isEmpty());
		Thread.sleep(500);
		ecsService.getECS().start();
		Thread.sleep(500);
		}
		catch(Exception exc){
			System.out.println("Init ECS failed " + exc.getMessage());
		}
		

		// Push all data to the first server
//		ServerInfo firstServer = getServerInfo(0);
//		KVStore kvClient = new KVStore(firstServer.getAddress(),
//				firstServer.getPort());
		try {
//			kvClient.connect();
//			for (String key : dictionary.dictionary.keySet()) {
//				kvClient.put(key, dictionary.dictionary.get(key));
//			}
//			kvClient.disconnect();
//			
			//one is already running
			for (int i = 1; i < serverNumber; i++) {
				ecsService.getECS().addNode();
			}
//			
		} catch (Exception e) {
			fail();
		}
		assertTrue(true);
	}

	
	@Test
	public void testRemoveNode(){
		Exception ex = null;
		try{
			ecsService.getECS().removeNode();
		}catch(Exception exc){
			assertNull("Exception in removing node occured", ex);
		}
	}
	@Test
	public void testAddNode(){
		Exception ex = null;
		try{
			ecsService.getECS().addNode();
		}catch(Exception exc){
			assertNull("Exception in adding node occured", ex);
		}
	}
	
	
	public ServerInfo getServerInfo(int i) {
		ServerInfo serverInfo = new ServerInfo("localhost", 6000);
		return serverInfo;
	}
	@Test
	public void testOneTimeTearDown() {
		System.out.println("--shutdown ");
		ecsService.getECS().shutdown();
	}

}
