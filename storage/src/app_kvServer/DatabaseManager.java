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

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import common.Hasher;
import common.messages.KVMessage;
import common.messages.ClientMessage;

public class DatabaseManager {

	private static Map < String , String > database = new HashMap < String , String > ();
	private static Logger logger = Logger.getRootLogger ();

	/**
	 * put a new tuple in the database
	 * 
	 * @param key
	 * @param value
	 * @return <code>KVMessage</code> return message representing the put status
	 */
	public static synchronized KVMessage put ( String key , String value ) {
		if ( key == null ) {

			ClientMessage msg = new ClientMessage ();
			msg.setValue ( "can not insert null key in the database" );
			msg.setStatus ( common.messages.KVMessage.StatusType.PUT_ERROR );
			logger.error ( "can not insert null key in the database" );

			return msg;

		} else if ( database.containsKey ( key ) ) {

			if ( value == null || value.equals ( "null" ) ) {
				return delete ( key );

			} else {
				return update ( key , value );
			}

		} else {
			ClientMessage msg = new ClientMessage ();
			try {
				database.put ( key , value );
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
	public static synchronized KVMessage get ( String key ) {
		ClientMessage msg = new ClientMessage ();
		try {
			String value = database.get ( key );
			if ( value == null ) {
				msg.setKey ( key );
				msg.setStatus ( common.messages.KVMessage.StatusType.GET_ERROR );
				msg.setValue ( "no value mapped to this key" );
				logger.error ( "Error in retrieving value because no value mapped to"
						+ " this key" );
			} else {
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
	private static KVMessage delete ( String key ) {
		ClientMessage msg = new ClientMessage ();
		try {
			database.remove ( key );
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
	private static KVMessage update ( String key , String value ) {
		ClientMessage msg = new ClientMessage ();
		try {
			database.put ( key , value );
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
	public synchronized static void putAll ( Map < String , String > data ) {
		database.putAll ( data );
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
	public static Map < String , String > getDataInRange ( String rangeStart ,
			String rangeEnd ) {
		Map < String , String > dataToBeMoved = new HashMap < String , String > ();
		Hasher hasher = new Hasher ();
		for ( String key : database.keySet () ) {
			if ( hasher.isInRange ( rangeStart , rangeEnd , key ) ) {
				dataToBeMoved.put ( key , database.get ( key ) );
			}
		}
		return dataToBeMoved;
	}

	/**
	 * deletes data (key-value) in the given range.
	 * 
	 * @param rangeStart
	 *            the hash representation of the start of the range
	 * @param rangeEnd
	 *            the hash representation of the end of the range
	 */
	public static void removeDataInRange ( String rangeStart , String rangeEnd ) {
		Hasher hasher = new Hasher ();
		for ( String key : database.keySet () ) {
			if ( hasher.isInRange ( rangeStart , rangeEnd , key ) ) {
				database.remove ( key );
			}
		}
	}

	public static void printDatabase () {
		System.out.println ( database.toString () );
	}

}
