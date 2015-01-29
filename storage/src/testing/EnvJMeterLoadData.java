package testing;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.junit.Test;

import common.messages.KVMessage;
import app_kvServer.DatabaseManager;

public class EnvJMeterLoadData extends TestCase {

	static {
		try {
			new LogSetup("test.log", Level.ALL);
		} catch (Exception exc) {

		}
		// Data set initialization
		String fileName = "british-english";

		Dictionary.loadDataIntoDictionary(fileName);
	}

	//
	@Test
	public void testLoadDataAtOnceToServer() {
		DatabaseManager db = new DatabaseManager(60000, "ser");
		db.putAll(Dictionary.dictionary);
		db.saveCacheToDatabase();
		assertTrue(true);
	}

	@Test
	public void testFindFuckingKey() {
		String bastard = "crèche's";
		String re = Dictionary.getValue(bastard);

		assertNotNull(re);

	}

	@Test
	public void testbastardKeyExistinTheDB() {
		String bastard = "crèche's";
		String bastardValue = Dictionary.getValue(bastard);
		DatabaseManager db = new DatabaseManager(60000, "ser");
		db.putAll(Dictionary.dictionary);
		KVMessage re = db.get(bastard);
		assertTrue(re.getValue().equals(bastardValue));
	}

	@Test
	public void testbastardKeyExistinTheDBWritten() {
		String bastard = "crche's";
		String bastardValue = Dictionary.getValue(bastard);

		List<String> keysAsArray = new ArrayList<String>(
				Dictionary.dictionary.keySet());
		
		DatabaseManager db = new DatabaseManager(60000, "ser");
		String sum="";
		for (String key : keysAsArray) {
			String dicVal = Dictionary.getValue(key);
			KVMessage re = db.get(bastard);
			String dbVal = re.getValue();
			
			if(!dbVal.equals(dicVal)){
				sum+=','+ key;
			}
			assertFalse(dbVal.isEmpty());
			assertFalse(dicVal.isEmpty());
			//assertTrue(dicVal.equals(dbVal));
		}
		/*
		 * DatabaseManager db = new DatabaseManager(6000,"ser"); db.putAll
		 * (Dictionary.dictionary); db.saveCacheToDatabase();
		 */

	}

}
