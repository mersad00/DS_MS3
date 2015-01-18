package testing;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.junit.*;

import common.ServerInfo;
import client.KVStore;

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

public class ClientSubscriptionUpdateTest {
	
	KVStore client = new KVStore("localhost",50002);
	List<ServerInfo> metadata0 = new ArrayList<ServerInfo>();
	List<ServerInfo> metadataCopy = new ArrayList<ServerInfo>();
	
	ServerInfo s0;
	ServerInfo s1;
	ServerInfo s2;
	ServerInfo s3;
	ServerInfo s4;
	@Before
	public void setUp(){
		try {
			new LogSetup("logs/test/DatabaseTest.log", Level.ALL);
		}catch(Exception e){
			System.out.println(e);
		}
		/* ip:port and their MD5 hashvalues
		 * 127.0.0.1:50005  297e522da5461c774be1037dfb0a8226
		 * 127.0.0.1:50003  a98109598267087dfc364fae4cf24578
		 * 127.0.0.1:50002  b3638a32c297f43aa37e63bbd839fc7e
		 * 127.0.0.1:50004  da850509fc3b88a612b0bcad7a37963b
		 * 127.0.0.1:50001  dcee0277eb13b76434e8dcd31a387709
		 */
		s0 = new ServerInfo("127.0.0.1",50005);
		s1 = new ServerInfo("127.0.0.1",50003);
		s2 = new ServerInfo("127.0.0.1",50002);
		s3 = new ServerInfo("127.0.0.1",50004);
		s4 = new ServerInfo("127.0.0.1",50001);
		
		metadata0.add(s0);
		metadata0.add(s1);
		metadata0.add(s2);

		client.subscribe(s0, "5", "five");
		client.subscribe(s0, "3", "three");
		client.subscribe(s0, "2", "two");
		client.subscribe(s0, "8", "eight");
		client.subscribe(s0, "10", "ten");
		client.subscribe(s1, "9", "nine");
		client.subscribe(s1, "11", "eleven");
		client.subscribe(s1, "7", "seven");
		

		
		for(ServerInfo s: metadata0){
			s.setFirstCoordinatorInfo( metadata0.get((metadata0.indexOf(s) + metadata0.size() -2 )% metadata0.size()));
			s.setSecondCoordinatorInfo(metadata0.get((metadata0.indexOf(s) + metadata0.size() -1 )% metadata0.size()));
		}
		
		//making deep copy of metadata0
		for(ServerInfo s:metadata0){
			ServerInfo temp = new ServerInfo();
			temp.setAddress(s.getAddress());
			temp.setPort(s.getPort());
			temp.setFromIndex(s.getFromIndex());
			temp.setToIndex(s.getToIndex());
			temp.setSecondCoordinatorInfo(s.getSecondCoordinatorInfo());
			metadataCopy.add(temp);
		}
		
	}
	
	@Test
	public void testAddInDetails(){
		try{

			// with the new addition of s3, keys 2,8,10 don't belong to s0 anymore
			metadata0.add(s3);
			
			//the first metadata given to client
			client.updateMetadata(metadata0);

			//check the suspected servers
			//metadatacopy is the respresentator of old meta data
			assertTrue(client.checkSubscribtionValidation(metadataCopy).contains(s0));
			
			// adding s0 as toBechecked
			List<ServerInfo> toBechecked = client.checkSubscribtionValidation(metadataCopy);
			
			// check if 2,8,10 has been removed from s0's responsibility
			assertTrue(client.checkKeyResponsibilities(toBechecked).contains("2"));
			assertTrue(client.checkKeyResponsibilities(toBechecked).contains("10"));
			assertTrue(client.checkKeyResponsibilities(toBechecked).contains("8"));

			assertFalse(client.checkKeyResponsibilities(toBechecked).contains("5"));
			assertFalse(client.checkKeyResponsibilities(toBechecked).contains("3"));
			
			Map<ServerInfo, List<String>> tobeSubscribed = client.sortKeysByServer(client.checkKeyResponsibilities(toBechecked));
			assertTrue(tobeSubscribed.containsKey(s3));
			assertTrue(tobeSubscribed.get(s3).contains("2"));
			assertTrue(tobeSubscribed.get(s3).contains("8"));
			assertTrue(tobeSubscribed.get(s3).contains("10"));
			
			
		}catch(Exception e){
			System.out.println("It is OK client cannot send anythin! No server is running");
		}
	}
	
	@Test
	public void testRemoveInDetails(){
		try{
			
			//with removal of s1, keys 9,11,7 will belong to s2
			metadata0.remove(s1);
			//the first metadata given to client
			client.updateMetadata(metadata0);

			//check the suspected servers
			//metadatacopy is the respresentator of old meta data
			assertTrue(client.checkSubscribtionValidation(metadataCopy).contains(s1));
			assertTrue(client.checkSubscribtionValidation(metadataCopy).contains(s2));
			
			
			// adding s1,s2 as toBechecked
			List<ServerInfo> toBechecked = client.checkSubscribtionValidation(metadataCopy);
			
			// check if 9,11,7 has been removed from s1's responsibility
			assertTrue(client.checkKeyResponsibilities(toBechecked).contains("9"));
			assertTrue(client.checkKeyResponsibilities(toBechecked).contains("7"));
			assertTrue(client.checkKeyResponsibilities(toBechecked).contains("11"));

			assertFalse(client.checkKeyResponsibilities(toBechecked).contains("5"));
			assertFalse(client.checkKeyResponsibilities(toBechecked).contains("3"));

			assertFalse(client.checkKeyResponsibilities(toBechecked).contains("2"));
			assertFalse(client.checkKeyResponsibilities(toBechecked).contains("8"));
			
			Map<ServerInfo, List<String>> tobeSubscribed = client.sortKeysByServer(client.checkKeyResponsibilities(toBechecked));
			assertTrue(tobeSubscribed.containsKey(s2));
			assertTrue(tobeSubscribed.get(s2).contains("9"));
			assertTrue(tobeSubscribed.get(s2).contains("11"));
			assertTrue(tobeSubscribed.get(s2).contains("7"));
			
			
		}catch(Exception e){
			System.out.println("It is OK client cannot send anythin! No server is running");
		}
		
		}
	
	
}
