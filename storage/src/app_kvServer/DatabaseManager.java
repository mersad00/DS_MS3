/**
 * This class represents the key-value store (Database)
 * which contains a map containing the key and value as 
 * a tuples.
 * 
 * <p> This class is a thread-safe class since multiple 
 * threads accessing it and perform concurrent modification
 * and reading operations.
 * 
 * @see KVMessage
 * @see ClientMessage
 * @see HashMap
 */

package app_kvServer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import common.Hasher;
import common.messages.KVMessage;
import common.messages.ClientMessage;

public class DatabaseManager {

	//private static Map < String , String > database = new HashMap < String , String > ();
	private Cache cache;
	private static Logger logger = Logger.getRootLogger ();
	private int id;
	private String dataBaseUri;
	
	public DatabaseManager(int id, int cacheSize,String cacheStrategy ,String databaseType ){
		
		/* path added in order to handle invocation by a remote process through ssh
		in order to avoid Filenotfound exception when creating Presistentstorage. 
		Because when this program
		is called from a remote process, the user directory will link to 
		"/home/<user>"
		 it is sufficient to delete path variable from this code when there is no
		 remote process calling this object
		*/
		this.id = id;
		String path = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
		cache = new Cache(cacheSize,Strategy.valueOf(cacheStrategy));
		/* for local invoking of the KVserver programs(no ssh call), we remove /bin to refer the path to
		project's root path*/ 
		path = path.replace("/bin", "");
		
		/* if the name of the jar file changed! this line of code must be updated
		for handling calls within ssh */
		path = path.replace("ms3-server.jar", "");
		
		this.dataBaseUri = path +"/PersistentStorage-"+ id + "."+ databaseType ; 
		File f = new File(dataBaseUri);
		
		if(!f.exists()){
			try{
				/* on the first run of the program the storage is created*/
					f.createNewFile();
					Map<String, String> temp = new HashMap<String,String>();
					FileOutputStream fileOut = new FileOutputStream(f,false);
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(temp);
					logger.info("Server's First time starting: Persistent storage created!");
					out.close();
					fileOut.close();
				}catch(IOException e){
		         logger.error("an error occured while trying to create the Storage");
		         logger.error(e.getMessage());
		      }
		}
	}
	
	/**
	 * put a new tuple in the database
	 * 
	 * @param key
	 * @param value
	 * @return <code>KVMessage</code> return message representing the put status
	 */
	@SuppressWarnings("unchecked")
	public synchronized KVMessage put ( String key , String value ) {
		if ( key == null ) {

			ClientMessage msg = new ClientMessage ();
			msg.setValue ( "can not insert null key in the database" );
			msg.setStatus ( common.messages.KVMessage.StatusType.PUT_ERROR );
			logger.error ( "can not insert null key in the database" );

			return msg;

		//} else if ( database.containsKey ( key ) ) {
		} else if ( contains(key) ) {

			if ( value == null || value.equals ( "null" ) ) {
				return delete ( key );

			} else {
				return update ( key , value );
			}

		} else {
			ClientMessage msg = new ClientMessage ();
			try {
				//database.put ( key , value );	
				savetoDatabase(key, value);
			    cache.push(key, value);
			         
				msg.setKey ( key );
				msg.setValue ( value );
				msg.setStatus ( common.messages.KVMessage.StatusType.PUT_SUCCESS );
				logger.info ( "insert for key: '" + key + "' and value '"
						+ value + "'  success" );
			} catch ( Exception e ) {
				msg.setStatus ( common.messages.KVMessage.StatusType.PUT_ERROR );
				msg.setKey ( key );
				msg.setValue ( e.getMessage () );
				logger.error ( "insert for key: '" + key
						+ "' failed because of :" + e.getMessage () );
			}
			return msg;
		}
	}

	/**
	 * get stored tuple from the database
	 * 
	 * @param key
	 * @return <code>KVMessage</code> return message representing the get status
	 */
	@SuppressWarnings("unchecked")
	public synchronized  KVMessage get ( String key ) {
		ClientMessage msg = new ClientMessage ();
		try {
			
			//String value = database.get ( key );
			String value = cache.get(key);
			
			// cache miss! loading the dataBase
			if( value == null)	
				value = loadFromDatabase(key); 
			    
			// in a case the key does not exists in our database
			 if ( value == null ) {
				msg.setKey ( key );
				msg.setStatus ( common.messages.KVMessage.StatusType.GET_ERROR );
				msg.setValue ( "no value mapped to this key" );
				logger.warn ( "Failed to retrieve the value because no value mapped to"
						+  key );
			}
			 else {
				msg.setKey ( key );
				msg.setValue ( value );
				msg.setStatus ( common.messages.KVMessage.StatusType.GET_SUCCESS );
				logger.info ( "retrieving value for key: '" + key + "' success" );
			}
		} catch ( Exception e ) {
			msg.setKey ( key );
			msg.setStatus ( common.messages.KVMessage.StatusType.GET_ERROR );
			msg.setValue ( "Error in retrieving value from database" );
			logger.error ( "Error in retrieving value because of :"
					+ e.getMessage () );
		}
		return msg;
	}

	/**
	 * delete tuple if the value is null
	 * 
	 * @param key
	 *            the key of the value
	 * @return KVMessage containing the status of the operation if failed or
	 *         succeed
	 */
	@SuppressWarnings("unchecked")
	private KVMessage delete ( String key ) {
		ClientMessage msg = new ClientMessage ();
		try {
			//database.remove ( key );
			deleteFromDatabase(key);
			cache.push(key, null);
			
			msg.setKey ( key );
			msg.setStatus ( common.messages.KVMessage.StatusType.DELETE_SUCCESS );
			logger.info ( "delete for key : '" + key + "' success" );
		} catch ( Exception e ) {
			msg.setKey ( key );
			msg.setStatus ( common.messages.KVMessage.StatusType.DELETE_ERROR );
			logger.info ( "delete for key : '" + key + "' failed because of :"
					+ e.getMessage () );
		}
		return msg;
	}

	/**
	 * updates the tuple in case of the key is already in the database
	 * 
	 * @param key
	 *            the key of the value
	 * @param value
	 *            the new value to be set
	 * @return KVMessage containing the status of the operation if failed or
	 *         succeed
	 */
	@SuppressWarnings("unchecked")
	private KVMessage update ( String key , String value ) {
		ClientMessage msg = new ClientMessage ();
		try {
			
			//database.put ( key , value );
			
			savetoDatabase(key, value);
		    cache.push(key, value);
			
			msg.setKey ( key );
			msg.setValue ( value );
			msg.setStatus ( common.messages.KVMessage.StatusType.PUT_UPDATE );
			logger.info ( "update for key: '" + key + "' and value '" + value
					+ "' success" );
		} catch ( Exception e ) {
			msg.setKey ( key );
			msg.setStatus ( common.messages.KVMessage.StatusType.PUT_ERROR );
			logger.error ( "insert for key: '" + key + "' failed because of"
					+ e.getMessage () );
		}
		return msg;
	}

	/**
	 * this methods insert a collection of data at once
	 * 
	 * @param data
	 *            data collection to be inserted
	 */
	@SuppressWarnings("unchecked")
	public synchronized void putAll ( Map < String , String > data ) {
		//database.putAll ( data );
		
	    // saving the new data into DataBase
	    try{
	    	//loading the DataBase
			Map<String, String> temp = new HashMap<String, String>();
			FileInputStream fileIn = new FileInputStream(this.dataBaseUri);
		    ObjectInputStream in = new ObjectInputStream(fileIn);
		    temp = (HashMap<String, String>) in.readObject();
			in.close();
		    fileIn.close();
		    //saving into dataBase
	    	temp.putAll(data);
			FileOutputStream fileOut = new FileOutputStream(this.dataBaseUri);
		    ObjectOutputStream out = new ObjectOutputStream(fileOut);
		    out.writeObject(temp);
		    out.close();
		    fileOut.close();
	    }catch(Exception e){
	    	logger.error(e.getMessage());
	    }
	}

	/**
	 * retrieves data (key-value) in the given range.
	 * 
	 * @param rangeStart
	 *            the hash representation of the start of the range
	 * @param rangeEnd
	 *            the hash representation of the end of the range
	 * @return Map containing the data in the range.
	 */
	@SuppressWarnings("unchecked")
	public synchronized Map < String , String > getDataInRange ( String rangeStart ,
			String rangeEnd ) {
		
		try{
			//loading the DataBase
			HashMap<String, String> temp = new HashMap<String, String>();
			FileInputStream fileIn = new FileInputStream(this.dataBaseUri);
		    ObjectInputStream in = new ObjectInputStream(fileIn);
		    temp = (HashMap<String, String>) in.readObject();
			in.close();
		    fileIn.close();
			
			Map < String , String > dataToBeMoved = new HashMap < String , String > ();
			Hasher hasher = new Hasher ();
			//for ( String key : database.keySet () ) {
			for ( String key : temp.keySet () ) {
				if ( hasher.isInRange ( rangeStart , rangeEnd , key ) ) {
					dataToBeMoved.put ( key , temp.get ( key ) );
				}
			}
			
			return dataToBeMoved;
		}catch(Exception e){
			logger.error("error occured while getting data"
					+ "in range " + e.getMessage());
			return null;
		}
	}

	/**
	 * deletes data (key-value) in the given range.
	 * 
	 * @param rangeStart
	 *            the hash representation of the start of the range
	 * @param rangeEnd
	 *            the hash representation of the end of the range
	 */
	@SuppressWarnings("unchecked")
	public synchronized void removeDataInRange ( String rangeStart , String rangeEnd ) {
		
		try{
			//loading the DataBase
			HashMap<String, String> temp = new HashMap<String, String>();
			FileInputStream fileIn = new FileInputStream(this.dataBaseUri);
		    ObjectInputStream in = new ObjectInputStream(fileIn);
		    temp = (HashMap<String, String>) in.readObject();
			in.close();
		    fileIn.close();
			
			//Map < String , String > dataToBeMoved = new HashMap < String , String > ();
			Hasher hasher = new Hasher ();
			

			/*
			 * Using Iterator and while loop.
			 * In this method you can remeove the entries
			 * while iterating through it.
			 *  The previous for loop will through
			 *   java.util.ConcurrentModificationException 
			 *   if you remove while iterating.
			 *
			for ( String key : database.keySet () ) {
				if ( hasher.isInRange ( rangeStart , rangeEnd , key ) ) {
					//database.remove(key);
					//temp.remove ( key);
				}
			*/
			
			Iterator<Map.Entry<String, String>> iterator = temp.entrySet().iterator() ;
	        while(iterator.hasNext()){
	            Map.Entry<String, String> item = iterator.next();
	            //You can remove elements while iterating.
	            if ( hasher.isInRange ( rangeStart , rangeEnd , item.getKey() ) ) {
					iterator.remove();
				}
			}
			
			//saving the changes to the persistent database
			FileOutputStream fileOut = new FileOutputStream(this.dataBaseUri);
		    ObjectOutputStream out = new ObjectOutputStream(fileOut);
		    out.writeObject(temp);
		    out.close();
		    fileOut.close();
	
		}catch(Exception e){
			logger.error("error occured while removing data"
					+ "in range " + e.getMessage());
			e.printStackTrace();

		}
	}

	@SuppressWarnings("unchecked")
	public synchronized void printDatabase () {
		try{
			HashMap<String, String> temp = new HashMap<String, String>();
			FileInputStream fileIn = new FileInputStream(this.dataBaseUri);
		    ObjectInputStream in = new ObjectInputStream(fileIn);
		    temp = (HashMap<String, String>) in.readObject();
			in.close();
		    fileIn.close();
			System.out.println ( temp.toString () );
		}catch(Exception e){
			logger.error("failed to print the dataBase! " + e.getMessage());
		}
	}
	
	/* additional methods added for handling input and output from the file */
	
	@SuppressWarnings("unchecked")
	private void savetoDatabase(String key, String value) throws IOException, ClassNotFoundException{
		
		//loading the DataBase
		HashMap<String, String> temp = new HashMap<String, String>();
		FileInputStream fileIn = new FileInputStream(this.dataBaseUri);
	    ObjectInputStream in = new ObjectInputStream(fileIn);
	    temp = (HashMap<String, String>) in.readObject();
		in.close();
	    fileIn.close();
	    
	    // saving the new data into DataBase
	    temp.put(key, value);
	    FileOutputStream fileOut = new FileOutputStream(this.dataBaseUri);
	    ObjectOutputStream out = new ObjectOutputStream(fileOut);
	    out.writeObject(temp);
	    out.close();
	    fileOut.close();
	}
	
	@SuppressWarnings("unchecked")
	private void deleteFromDatabase(String key) throws IOException, ClassNotFoundException{
		
		//loading the DataBase
		HashMap<String, String> temp = new HashMap<String, String>();
		FileInputStream fileIn = new FileInputStream(this.dataBaseUri);
	    ObjectInputStream in = new ObjectInputStream(fileIn);
	    temp = (HashMap<String, String>) in.readObject();
		in.close();
	    fileIn.close();
	    
	    // saving the new data into DataBase
	    temp.remove(key);
	    FileOutputStream fileOut = new FileOutputStream(this.dataBaseUri);
	    ObjectOutputStream out = new ObjectOutputStream(fileOut);
	    out.writeObject(temp);
	    out.close();
	    fileOut.close();
	}
	
	
	@SuppressWarnings("unchecked")
	private String loadFromDatabase(String key) throws IOException, ClassNotFoundException{
		
		//loading the DataBase
		HashMap<String, String> temp = new HashMap<String, String>();
		FileInputStream fileIn = new FileInputStream(this.dataBaseUri);
	    ObjectInputStream in = new ObjectInputStream(fileIn);
	    temp = (HashMap<String, String>) in.readObject();
		in.close();
	    fileIn.close();
	    return temp.get(key);
	   
	}
	
	@SuppressWarnings({ "unchecked", "null" })
	private boolean contains(String key){
			try{
				HashMap<String, String> temp = new HashMap<String, String>();
				FileInputStream fileIn = new FileInputStream(this.dataBaseUri);
			    ObjectInputStream in = new ObjectInputStream(fileIn);
			    temp = (HashMap<String, String>) in.readObject();
				in.close();
			    fileIn.close();
			    return temp.containsKey(key);
			}catch(Exception e){
				return false;
			}
				
	}

	public Map <String, String> getAll() throws IOException, ClassNotFoundException{
		Map<String, String> temp = new HashMap<String, String>();
		FileInputStream fileIn = new FileInputStream(this.dataBaseUri);
	    ObjectInputStream in = new ObjectInputStream(fileIn);
	    temp = (Map<String, String>) in.readObject();
		in.close();
	    fileIn.close();
	    return temp;
	}
}