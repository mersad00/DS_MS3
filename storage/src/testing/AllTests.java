package testing;

import java.io.IOException;

import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import client.KVStore;

import app_kvEcs.ECSMock;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;

import static org.junit.Assert.assertTrue;

@RunWith(Suite.class)
@SuiteClasses({
	ConnectionTest.class,
	InteractionTest.class,
	AdditionalTest.class
})
public class AllTests {
	
	private static KVStore kvClient;
	private static ECSMock ecs;
	
	@BeforeClass
	public static void setUp() {		
			Exception ex = null;
			try {
				new LogSetup("logs/testing/test.log", Level.ERROR);
				kvClient = new KVStore("localhost", 50000);	
				ecs = new ECSMock ( 1 , "ecs.config" );
				ecs.getECS ().start ();
				kvClient.connect();	
			} catch (IOException e) {
				ex = e;
			}
			assertTrue ( ex == null );			
		
	}

	@AfterClass
	public static void tearDown() {
		Exception ex  = null;
		try{
			kvClient.disconnect();
			ecs.getECS ().shutdown ();
		} catch ( Exception e){
			ex = e;
		}
		assertTrue ( ex == null );	
	}
	
	
	
	
	
}
