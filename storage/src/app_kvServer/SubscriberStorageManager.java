package app_kvServer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import client.ClientInfo;


public class SubscriberStorageManager {
	
	/* This Map contains all registered for keys and all clients registered to this key */
	private Map<String, List<ClientInfo>> subscribtions;
	
	public SubscriberStorageManager(){
		this.subscribtions = new HashMap<String, List<ClientInfo>>();
	}
	
	/*
	 * add subscriber to specific key
	 */
	public void addSubscriber(String key, ClientInfo subscriber){
		List<ClientInfo> subscribersList = this.getKeySubscribers(key);
		if(subscribersList == null){
			subscribersList = new ArrayList<ClientInfo>();
			this.subscribtions.put(key, subscribersList);
		} 
		// add it to the list of subscribers, if it is not already subscribed to t!
		if(! subscribersList.contains(subscriber))
			subscribersList.add(subscriber);
	}
	
	
	/*
	 * remove subscriber for specific key
	 */
	public void removeSubscriber(ClientInfo subscriber, String key){
		List<ClientInfo> subscribersList = this.getKeySubscribers(key);
		if(subscribersList != null){
			subscribersList.remove(subscriber);
		} 
	}
	
	/*
	 * get all subscribers for specific key
	 */
	public List<ClientInfo> getKeySubscribers (String key){
		return this.subscribtions.get(key);
	}
}
