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
	
	public  void subscribetoServer(ServerInfo server,String key, String value){
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
	
	public  List<String> getSubscribedKeys(ServerInfo server){
			return subscribtionList.get(server);
	}
	
	public  List<ServerInfo> getSubscribedServers(){
		ArrayList<ServerInfo> temp = new ArrayList<ServerInfo>();
		for(ServerInfo s: subscribtionList.keySet()){
			temp.add(s);
		}
		return temp;
	}
	
	
	public  void cleanServerList(List<ServerInfo> newMetaData){
		for(ServerInfo s: newMetaData){
			if(! this.subscribtionList.containsKey(s))
				this.subscribtionList.remove(s);
		}
	}
		
		
	public void remove(String key, ServerInfo responsible){
		ArrayList<String> keys = this.subscribtionList.get(responsible);
		keys.remove(key);
		this.subscribtionList.put(responsible, keys);
		remove(key);
	}
		

}
