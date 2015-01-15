package client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import common.Cache;
import common.CacheStrategy;
import common.ServerInfo;

public class SubscribtionCache extends Cache{
	private HashMap<ServerInfo, ArrayList<String>> subscribtionList;
	
	public SubscribtionCache(int size, CacheStrategy strategy) {
		super(size, strategy);
		subscribtionList = new HashMap<ServerInfo, ArrayList<String>>();
	}
	
	public synchronized void subscribetoServer(ServerInfo server,String key, String value){
		ArrayList<String> temp;
		
		if(subscribtionList.containsKey(server)){
			temp = subscribtionList.get(server);
			temp.add(key);
			subscribtionList.put(server, temp);
		}else{
			temp = new ArrayList<String>();
			temp.add(key);
			subscribtionList.put(server, temp);
		}
		// pushing the key into the cache storage
		push(key,value);
	}
	
	public synchronized List<String> getSubscribedKeys(ServerInfo server){
			return subscribtionList.get(server);
	}
	
	public synchronized List<ServerInfo> getSubscribedServers(){
		ArrayList<ServerInfo> temp = new ArrayList<ServerInfo>();
		System.arraycopy(subscribtionList.keySet(), 0, temp, 0, subscribtionList.size());
		return temp;
	}
	
	
	public synchronized void cleanServerList(List<ServerInfo> newMetaData){
		for(ServerInfo s: newMetaData){
			if(! this.subscribtionList.containsKey(s))
				this.subscribtionList.remove(s);
		}
//TODO add method remove for unsibscribing
		/*
	@Override
	public synchronized remove(String key){
		this.subscribtionList.keySet()
	}*/
	
	}	

}
