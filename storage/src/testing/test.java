package testing;

import common.Hasher;

public class test {
	public static void main (String args[]){
		Hasher h = new Hasher ();
		System.out.println ( h.isInRange ( "2B786438D2C6425DC30DE0077EA6494D" , "0221F85727F09BB279FA843D25C48052" , h.getHash ( "key1" ) )
				);
		System.out.println ( h.isInRange ( "0221F85727F09BB279FA843D25C48052" , "2B786438D2C6425DC30DE0077EA6494D" , h.getHash ( "key1" ) )
				);
	}
}
