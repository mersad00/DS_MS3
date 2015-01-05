package testing;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.util.HashMap;

import app_kvClient.UserFacingMessages;

public class DataBaseReader {
	private int id;
	private String databaseType;
	private String dataBaseUri;

	public DataBaseReader(int id, String databaseType) {
		this.id = id;
		String path = this.getClass().getProtectionDomain().getCodeSource()
				.getLocation().getPath();
		/*
		 * for local invoking of the KVserver programs(no ssh call), we remove
		 * /bin to refer the path to project's root path
		 */
		path = path.replace("/bin", "");

		/*
		 * if the name of the jar file changed! this line of code must be
		 * updated for handling calls within ssh
		 */
		path = path.replace("ms3-server.jar", "");

		this.dataBaseUri = path + "/PersistentStorage-" + id + "."
				+ databaseType;
	}

	public synchronized void printDatabase() {
		try {
			HashMap<String, String> temp = new HashMap<String, String>();
			FileInputStream fileIn = new FileInputStream(this.dataBaseUri);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			temp = (HashMap<String, String>) in.readObject();
			in.close();
			fileIn.close();
			System.out.println(temp.toString());
		} catch (Exception e) {
			System.out.println("failed to print the dataBase! "
					+ e.getMessage());
		}
	}

	public static void main(String[] args) {
		BufferedReader cons = new BufferedReader(new InputStreamReader(
				System.in));
		// the flag to stop shell interaction
		boolean quit = false;
		while (!quit) {
			System.out.print("DataBaseReader>");
			String input;
			String[] tokens;
			try {
				input = cons.readLine();
				tokens = input.trim().split(UserFacingMessages.SPLIT_ON);
				// user input was split as tokens.
				// safety check
				if (tokens == null || tokens.length == 0) {
					throw new IllegalArgumentException(
							UserFacingMessages.GENERAL_ILLIGAL_ARGUMENT);
				}else if(tokens[0].equals("quit"))
					quit = true;
				else if(tokens.length >= 2){
					/*DataBaseReader dbr = new DataBaseReader(
						Integer.parseInt(tokens[0]), tokens[1]);
					*/
					DataBaseReader dbr = new DataBaseReader(
							Integer.parseInt(tokens[0]), "ser");
					dbr.printDatabase();

					DataBaseReader dbr1 = new DataBaseReader(
							Integer.parseInt(tokens[0]), "rep1");
					dbr1.printDatabase();

					DataBaseReader dbr2 = new DataBaseReader(
							Integer.parseInt(tokens[0]), "rep2");
					dbr2.printDatabase();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
}
