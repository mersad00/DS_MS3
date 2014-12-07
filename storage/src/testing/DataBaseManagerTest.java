package testing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;

import app_kvServer.DatabaseManager;

class Serverstub implements Runnable{
	private DatabaseManager dataBase;
	public Serverstub(DatabaseManager db) {
		this.dataBase = db;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		boolean running = true;
		while(running){
			dataBase.put("CM","Captain");
			dataBase.put("A","Absolute");
			dataBase.put("J","Jack");
			try {
				Thread.sleep(1000);
				dataBase.get("A");
				dataBase.get("1");
				dataBase.get("10");
				running = false;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
}
public class DataBaseManagerTest {
	
	DatabaseManager db = new DatabaseManager(12345, 10, "FIFO");
	Map <String	,String>dataSet = new HashMap<String, String>();
	
	@Before public void setUpData(){
		try {
			new LogSetup("logs/test/DatabaseTest.log", Level.ALL);
		}catch(Exception e){
			System.out.println(e);
		}
		dataSet.put("1", "Kahn");
		dataSet.put("2", "Mahdavikia");
		dataSet.put("3", "Maldini");
		dataSet.put("4", "Kuffur");
		dataSet.put("5", "Der Kaiser");
		dataSet.put("6", "Carlos");
		dataSet.put("7", "Beckham");
		dataSet.put("8", "Karimi");
		dataSet.put("9", "Lewa");
		dataSet.put("10", "Zidane");
		dataSet.put("31", "Schweini");		
	}
	
	@Test public void testPut(){
		db.putAll(dataSet);
		db.printDatabase();
		db.put("11", "Azizi");
		db.put("10", "Messi");
		db.put("2", null);
		db.put("5", "Beckenbauer");
		db.printDatabase();
		assertTrue( db.get("8").getValue().equals("Karimi") );
		assertTrue( db.get("3").getValue().equals("Maldini") );
		assertTrue( db.get("11").getValue().equals("Azizi") );
		assertTrue( db.get("5").getValue().equals("Beckenbauer") );
		assertTrue( db.get("2").getValue().equals("no value mapped to this key") );
	}
	
	@Test public void concurrencyTest(){
		Serverstub s1 = new Serverstub(db);
		Serverstub s2 = new Serverstub(db);
		Serverstub s3 = new Serverstub(db);
		Serverstub s4 = new Serverstub(db);
		Serverstub s5 = new Serverstub(db);
		Serverstub s6 = new Serverstub(db);
		s1.run();
		s2.run();
		s3.run();
		s4.run();
		s5.run();
		s6.run();
	}
	
}