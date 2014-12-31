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
	
	DatabaseManager db = new DatabaseManager(12345, 10, "FIFO","ser");
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
	
	
	/**
	 * keys sorted by their hashMD5 funtion
	 * 6 = 1679091c5a880faf6fb5e6087eb1b2dc
	 * 9 = 45c48cce2e2d7fbdea1afc51c7c6ad26
	 * 11= 6512bd43d9caa6e02c990b0a82652dca
	 * 7 = 8f14e45fceea167a5a36dedd4bea2543
	 * 4 = a87ff679a2f3e71d9181a67b7542122c
	 * 31 = c16a5320fa475530d9583c34fd356ef5
	 * 1 = c4ca4238a0b923820dcc509a6f75849b
	 * 2 = c81e728d9d4c2f636f067f89cc14862c
	 * 8 = c9f0f895fb98ab9159f51fd0297e236d
	 * 10 = d3d9446802a44259755d38e6d163e820
	 * 5 = e4da3b7fbbce2345d7772b0674a318d5
	 * 3 = eccbc87e4b5ce2fe28308fd9f2a7baf3
	 */
	@Test public void rangeTests(){
		Map <String,String> temp = 
				db.getDataInRange("a87ff679a2f3e71d9181a67b7542122c", "d3d9446802a44259755d38e6d163e820");
		db.removeDataInRange("c81e728d9d4c2f636f067f89cc14862c", "e4da3b7fbbce2345d7772b0674a318d5");
		
		db.printDatabase();
		assertFalse(temp.containsKey("4"));
		assertTrue(temp.containsKey("31"));
		assertFalse(temp.containsKey("9"));
		
		assertTrue( db.get("3").getValue().equals("Maldini") );
		assertTrue( db.get("1").getValue().equals("Kahn") );
		assertFalse( db.get("8").getValue().equals("Karimi") );
		System.out.println("<<< "+db.get("10").getValue() +">>>");
		assertTrue( db.get("10").getValue().equals("no value mapped to this key") );

		
		
		
	}
	
}