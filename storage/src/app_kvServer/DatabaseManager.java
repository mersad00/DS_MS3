package app_kvServer;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.messages.Message;

public class DatabaseManager {
	
	private static Map<String, String> database = new HashMap<String,String>();
	private static Logger logger = Logger.getRootLogger();
	
	public static synchronized KVMessage put(String key, String value){
		
		if (key == null){
			
			Message msg = new Message();
			msg.setValue("can not insert null key in the database");
			msg.setStatus(common.messages.KVMessage.StatusType.PUT_ERROR);
			logger.error("can not insert null key in the database");
			
			return msg;
			
		} else if ( database.containsKey(key)){
			
			if( value == null || value.equals("null")) {
				return delete(key);
				
			} else {				
				return update(key, value);
			}
			
		} else {
			Message msg = new Message();
			try{	
				database.put(key, value);
				msg.setKey(key);
				msg.setValue(value);
				msg.setStatus(common.messages.KVMessage.StatusType.PUT_SUCCESS);
				logger.info("insert for key: '" +  key +"' and value '"+value+"'  success");				
			}catch (Exception e) {
				msg.setStatus(common.messages.KVMessage.StatusType.PUT_ERROR);
				msg.setKey(key);
				msg.setValue(e.getMessage());
				logger.error("insert for key: '" +  key +"' failed because of :"
						     + e.getMessage());
			}			
			return msg;		
		}				
	}
	
	
	
	public static synchronized KVMessage get(String key){
		Message msg = new Message();
		try{
			String value = database.get(key);
			if(value==null){
				msg.setKey(key);
				msg.setStatus(common.messages.KVMessage.StatusType.GET_ERROR);
				msg.setValue("no value mapped to this key");
				logger.error("Error in retrieving value because no value mapped to" +
						     " this key");
			}else {
				msg.setKey(key);
				msg.setValue(value);
				msg.setStatus(common.messages.KVMessage.StatusType.GET_SUCCESS);
				logger.info("retrieving value for key: '"+key+"' success");
			}
		}catch (Exception e){
			msg.setKey(key);
			msg.setStatus(common.messages.KVMessage.StatusType.GET_ERROR);
			msg.setValue("Error in retrieving value from database");
			logger.error("Error in retrieving value because of :" +
				          e.getMessage());
		}
		return msg;
	}
	
	
	private static KVMessage delete(String key){
		Message msg = new Message();
		try{
			database.remove(key);				
			msg.setKey(key);			
			msg.setStatus(common.messages.KVMessage.StatusType.DELETE_SUCCESS);
			logger.info("delete for key : '"+key+"' success");
		}catch(Exception e){
			msg.setKey(key);
			msg.setStatus(common.messages.KVMessage.StatusType.DELETE_ERROR);
			logger.info("delete for key : '"+key+"' failed because of :"
					    + e.getMessage());
		}		
		return msg;
	}
	
	private static KVMessage update(String key, String value){
		Message msg = new Message();
		try{
			database.put(key,value);				
			msg.setKey(key);
			msg.setValue(value);
			msg.setStatus(common.messages.KVMessage.StatusType.PUT_UPDATE);
			printDatabase();
			logger.info("update for key: '"+key+"' and value '"+value+"' success");
		}catch(Exception e){
			msg.setKey(key);
			msg.setStatus(common.messages.KVMessage.StatusType.PUT_ERROR);
			logger.error("insert for key: '"+key+"' failed because of"
					     + e.getMessage());
		}		
		return msg;		
	}
	
	public static void printDatabase(){
		System.out.println(database.toString());
	}
	
}
