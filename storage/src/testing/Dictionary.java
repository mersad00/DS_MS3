package testing;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;


public class Dictionary {
    public static Map<String,String> dictionary = new HashMap<String, String>();
    
    public static void loadDataIntoDictionary(String fileName){
        File f = new File(fileName);
        
            try {                            
                Map<String, String> temp = new HashMap<String, String>();
                Scanner scanner = new Scanner(f);
                while(scanner.hasNext()){
                    String key = scanner.nextLine();
                    String val = scanner.nextLine();
                    if(key!=null && val!=null && !key.isEmpty() && !val.isEmpty() && !key.contains("è") && !val.contains("è")  )
                    	dictionary.put(key, val);                    
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    
    public static String getRandomKey(){
    	List<String> keysAsArray = new ArrayList<String>(dictionary.keySet());
    	Random r = new Random();
    	return keysAsArray.get(r.nextInt(keysAsArray.size()));
    } 
    public static String getIndexKey(int i){
    	List<String> keysAsArray = new ArrayList<String>(dictionary.keySet());
    	return keysAsArray.get(i);
    }
    
    public static String getValue(String Key){
    	return dictionary.get(Key);
    }
    
    
}