package common;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

/**
 * This class is used to handle hashing operations,
 * starting with initialization of <code>MD5</code> hasher.
 * It also provide method to compare two hashes and
 * check if a given key is located in a given hash
 * range.
 * 
 * <p> The <code>getHash(String key)</code> method get a string
 * as a parameter and return a hash string on the base 16. 
 * 
 * @exception NoSuchAlgorithmException if a MessageDigestSpi
 *            implementation for the specified algorithm is not available
 *            from the specified Provider object.
 *            
 * @see MessageDigest
 * @see HexBinaryAdapter
 */

public class Hasher {

	private MessageDigest md5;
	private HexBinaryAdapter adapter;
	private int base =16;
		
	
	public Hasher() {
		try {
			md5 = MessageDigest.getInstance("MD5");
			adapter = new HexBinaryAdapter();
		} catch ( NoSuchAlgorithmException e ) {
			e.printStackTrace();
		}		
		
	}
	/**
	 * this method generates a hash value for a given string
	 * using MD5 hashing algorithm
	 * @param key		the key to be hashed
	 * @return String	the hash value 
	 */
	public String getHash(String key) {
		byte [] array = md5.digest(key.getBytes());
		String hex = adapter.marshal(array);
		return hex;
	}
	
	/**
	 * this method compare two hash values
	 * @param      firstHash		
	 * @param      secondHash
	 * @return     0 in case of equal, 1 if first hash greater than the second
	 * 		       hash, -1 in case the first hash less than the second hash.
	 */
	public int compareHashes(String firstHash, String secondHash) {
		BigInteger firstValue = new BigInteger(firstHash, base);
		BigInteger secondValue = new BigInteger(secondHash, base);
		
		return firstValue.compareTo(secondValue);
	}
	
	
	/**
	 * this method determine if a given key is located in given range
	 * @param startRange 	the start hash value of the range
	 * @param endRange 		the end hash value of the range
	 * @param key			the key to be checked
	 * @return				<code>boolean</code> representing if the key is 
	 * 						in the range or not
	 */
	public boolean isInRange (String startRange, String endRange, String key){
		boolean result = false;
	    String keyHash = this.getHash ( key );
	    
	    /* in this case the key is between the last node and the first node */
	    if ( this.compareHashes ( startRange , endRange ) > 0) { 
	    	
	    	/* if (key < end) ---->  the key is between 000 and the first node */
	    	if ( this.compareHashes ( keyHash , endRange ) <= 0 ){ 
	    		result = true;
	    		
	    		/* in this case key is between last node and FFFF */
	    	} else if ( this.compareHashes ( keyHash , startRange ) > 0){ 
	    		result = true;
	    	}
	    	/* in case of only one node in the ring */
	    } else if ( this.compareHashes ( startRange , endRange ) == 0){
	    	result = true;
	    } else {
	    	
	    	if ( (this.compareHashes ( startRange , keyHash) <  0 )&& (this.compareHashes ( endRange , keyHash ) >= 0)){
		    	result = true;
		    }
	    }
	    
		return result;
	}
	
}
