package testing;
import java.io.IOException;

import junit.framework.TestCase;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import app_kvServer.DatabaseManager;

public class EnvJMeterLoadData extends TestCase{

	static {
		try{
			new LogSetup("test.log", Level.ALL);
		}catch(Exception exc){
			
		}
			// Data set initialization
			String fileName = "british-english";

			Dictionary.loadDataIntoDictionary(fileName);
		}
	
	@Test
	public void testLoadDataAtOnceToServer(){
		DatabaseManager db = new DatabaseManager(6000,100,"FIFO","ser");
		//for (String key : Dictionary.dictionary.keySet()) {
			db.putAll (Dictionary.dictionary);
		//}
		assertTrue(true);
	}
	
	
	

}
