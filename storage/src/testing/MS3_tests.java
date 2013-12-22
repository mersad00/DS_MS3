package testing;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import common.messages.ClientMessage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

import client.KVStore;

import app_kvEcs.ECSMock;

public class MS3_tests {
	
	
	private static KVStore client;
	private static ECSMock ecs;
	
	@Before
	public  void setUp(){		
		try {			
			ecs = new ECSMock ( 2 , "ecs.config" );
			client = new KVStore ( "localhost" , 50000 );
			client.connect ();
		} catch ( IOException e ) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testServerStopped(){
		Exception ex = null;
		KVMessage response= null;
		try {
			ecs.getECS ().stop ();
			Thread.sleep ( 1000 );
			response = client.put ( "key1" , "value1" );	
		} catch ( IOException e ) {
			ex = e;
		} catch ( InterruptedException e ) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus ().equals ( StatusType.SERVER_STOPPED ));
	}
	
	@Test
	public void testServerActive(){
		Exception ex = null;
		KVMessage response= null;
		try {
			ecs.getECS ().start ();
			Thread.sleep ( 1000 );
			response = client.put ( "key1" , "value1" );	
			ecs.getECS ().stop ();
		} catch ( IOException e ) {
			ex = e;
		} catch ( InterruptedException e ) {
			ex = e;
		}
		assertTrue((ex == null) && (!response.getStatus ().equals ( StatusType.SERVER_STOPPED )));
	}
	
	@Test
	public void testUpdateMetadata(){
		Exception ex = null;
		KVMessage response= null;
		ClientMessage clientMessage  = null;
		try {
			ecs.getECS ().start ();
			Thread.sleep ( 1000 );
			response = client.put ( "key1" , "value1" );	
			clientMessage = (ClientMessage)response;
		} catch ( IOException e ) {
			ex = e;
		} catch ( InterruptedException e ) {
			ex = e;
		}
		assertTrue((ex == null) && (clientMessage.getMetadata ().size ()!= 0));
	}
	
	@Test
	public void testServerNotResponsible(){
		Exception ex = null;
		KVMessage response= null;
		try {
			ecs.getECS ().start ();			
			Thread.sleep ( 1000 );
			response = client.put ( "key1" , "value1" );	
		} catch ( IOException e ) {
			ex = e;
		} catch ( InterruptedException e ) {
			ex = e;
		}
		assertTrue((ex == null) && (response.getStatus ().equals ( StatusType.SERVER_NOT_RESPONSIBLE)));
	}
	
	
	
	@After
	public void tearDown(){
		client.disconnect();
		ecs.getECS ().shutdown ();
		ecs = null;
		client = null;
	}

}
