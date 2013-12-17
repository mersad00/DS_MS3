package common;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;


public class Hasher {

	private MessageDigest md5;
	private HexBinaryAdapter adapter;
	private int base =16;
	
	
	public Hasher() {
		try {
			md5 = MessageDigest.getInstance("MD5");
			adapter = new HexBinaryAdapter();
		} catch ( NoSuchAlgorithmException e ) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
	}
	
	public String getHash(String key) {
		byte [] array = md5.digest(key.getBytes());
		String hex = adapter.marshal(array);
		return hex;
	}
	
	
//	public String getAppropriateStorageServerHash ( String key , Set<String> serverKeysHashes ) {
//		String destinationServer ="";
//		List<String> tempSortList = new ArrayList<String> ( serverKeysHashes );
//		Collections.sort ( tempSortList );
//		for ( String currentServerHash : tempSortList ){			
//			if ( compareHashes(currentServerHash, key) >= 0){
//				return currentServerHash;
//			}
//		}
//		//TODO in case of no server is in the range 
//		//TODO in case empty set passed
//		return tempSortList.get ( 0 );
//	}
	
	public int compareHashes(String firstHash, String secondHash) {
		BigInteger firstValue = new BigInteger(firstHash, base);
		BigInteger secondValue = new BigInteger(secondHash, base);
		
		return firstValue.compareTo(secondValue);
	}
	
	public boolean isInRange (String startRange, String endRange, String key){
		//TODO ask amjad about the range exclusive or inclusive
		boolean result = false;
	    String keyHash = this.getHash ( key );
	    System.out.println (startRange);
	    System.out.println (endRange);
	    System.out.println (keyHash);
	    if ( this.compareHashes ( startRange , endRange ) > 0) { // in this case the key is between the last node and the first node
	    	if ( this.compareHashes ( keyHash , endRange ) < 0 ){ // if (key < end) ---->  the key is between 000 and the first node
	    		result = true;
	    	} else if ( this.compareHashes ( keyHash , startRange ) > 0){ // in this case key is between last node and fff
	    		result = true;
	    	}
	    } else {
	    	if ( this.compareHashes ( startRange , keyHash) >=  0 && this.compareHashes ( endRange , keyHash ) <= 0){
		    	result = true;
		    }
	    }
	    
		return result;
	}
	
}
