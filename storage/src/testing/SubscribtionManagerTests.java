package testing;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import client.ClientInfo;
import app_kvServer.SubscriberStorageManager;
import junit.framework.TestCase;

public class SubscribtionManagerTests extends TestCase {

	@Test
	public void testAddSubscriptionForKey() {
		SubscriberStorageManager manager = new SubscriberStorageManager();

		final String mockKey = "mockKey";
		final String mockValue = "mockValue";
		final String mockAddress = "mockAddress";
		final int mockPort = 10;

		final String mockAddress1 = "mockAddress1";
		final int mockPort1 = 11;

		final ClientInfo mockSubscriber = new ClientInfo();
		mockSubscriber.setAddress(mockAddress);
		mockSubscriber.setPort(mockPort);

		final ClientInfo mockSubscriber1 = new ClientInfo();
		mockSubscriber1.setAddress(mockAddress1);
		mockSubscriber1.setPort(mockPort1);

		manager.addSubscriber(mockKey, mockSubscriber);
		manager.addSubscriber(mockKey, mockSubscriber1);

		List<ClientInfo> subscribersList = manager.getKeySubscribers(mockKey);
		assertTrue(subscribersList.contains(mockSubscriber));
		assertTrue(subscribersList.contains(mockSubscriber1));
	}

	@Test
	public void testRemoveSubscriptionForKey() {
		SubscriberStorageManager manager = new SubscriberStorageManager();

		final String mockKey = "mockKey";
		final String mockValue = "mockValue";
		final String mockAddress = "mockAddress";
		final int mockPort = 10;

		final String mockAddress1 = "mockAddress1";
		final int mockPort1 = 11;

		final ClientInfo mockSubscriber = new ClientInfo();
		mockSubscriber.setAddress(mockAddress);
		mockSubscriber.setPort(mockPort);

		final ClientInfo mockSubscriber1 = new ClientInfo();
		mockSubscriber1.setAddress(mockAddress1);
		mockSubscriber1.setPort(mockPort1);

		manager.addSubscriber(mockKey, mockSubscriber);
		manager.addSubscriber(mockKey, mockSubscriber1);

		List<ClientInfo> subscribersList = manager.getKeySubscribers(mockKey);
		assertTrue(subscribersList.contains(mockSubscriber));
		assertTrue(subscribersList.contains(mockSubscriber1));
		
		manager.removeSubscriber(mockSubscriber1, mockKey);
		assertTrue(!subscribersList.contains(mockSubscriber1));
	}

}
