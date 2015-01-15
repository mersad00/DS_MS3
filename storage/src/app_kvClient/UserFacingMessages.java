package app_kvClient;

/**
 * @author Amjad 
 * The class contains all the user facing strings of the application.
 */
public class UserFacingMessages {

	
	public static final String ECHO_PROMPT = "KVClient> ";
	
	public static final String ECS_ECHO_PROMPT = "ECS> ";
	
	public static final String SPLIT_ON = "\\s+";
	
	public static final String HELP_TEXT = "KVClient:  The second phase of cloud database course project."
			+ "\nUsage:"
			+ "\nConnect <address> <port>: Tries to establish a TCP- connection to the KV server based on the given server address and the port number of the echo service.\n"
			+ "address: Hostname or IP address of the KV server.\nport: The port of the KV service on the respective server.\n"
			+ " Example: connect 192.168.50.1 50000 \n"
			+ "disconnect: Tries to disconnect from the connected server."
			+ "\nget <key> : gets the value mapped to the key form the storage server data structure.\n"
			+ "\ngets <key> : gets the value mapped to the key form the storage server data structure with subscribtion to get notified of any changes made to the value.\n"
			+ "\nput <key> <value>: Inserts (or updtaes if it exists) a key-value pair into the storage server data structure."
			+ "\nputs <key> <value>: Inserts (or updtaes if it exists) a key-value pair into the storage server data structure with subscribtion to get notified of any changes made to the value.\n"
			+ "\nlogLevel <level>: Sets the logger to the specified log level."
			+ "\nlevel: One of the following log4j log levels: (ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF)."
			+ "\nHelp: Prints the help and usage tips."
			+ "\nquit: Tears down the active connection to the server and exits the program execution.\n"
			+ "\nunsubscribe <key> : tell the cloud server to unsibsribe this client from the key .\n";
	
	public static final String ECS_HELP_TEXT = "ECS:  The third phase of cloud database course project."
		+ "\nUsage:"
		+ "\nStart: Sends a start signal to all the servers under ECS control.\n"
		+ "Stop:  Sends a stop signal to all the servers under ECS control.\n"
		+ "Shutdown:  Sends a Shutdown signal to all the servers under ECS control.\n"
		+ "Add:  Adds a node to the ring and performs related data arrangements.\n"
		+ "Remove:  removes a node to the ring and performs related data arrangements.\n"
		+ "\nlogLevel <level>: Sets the logger to the specified log level."
		+ "\nHelp: Prints the help and usage tips."
		+ "\nquit: Shuts down servers and exit.";

	
	public static final String GENERAL_ILLIGAL_ARGUMENT = "Please enter a valid command. \n"+ HELP_TEXT;
	
	public static final String ILLIGAL_PARAM_NUMBER = "The number of the command parameters is not as expected, Please use the help command to see an example.";
	
	public static final String ILLIGAL_PARAM = " is not as expected, Please use the help command to see an example.";
	
	public static final String ILLIGAL_PARAM_PORT = "Port number in invalid, Please use the help command to see an example.";
	
	public static final String UN_SUPPORTED_COMMAND = "Unknown command.\n"+ HELP_TEXT;
	
	public static final String GET_ERROR_MESSAGE = "The get command was not successful:";
	
	public static final String GET_SUCCESS_MESSAGE = "The get command was successful, The value received:";
	public static final String GETS_SUCCESS_MESSAGE = "The get and subscirbe command was successful, The value received:";
	public static final String NOT_CONNECTED_YET = "Client is not connected yet! Please first connect to a server and try again.";
	public static final String PUT_ERROR_MESSAGE = "The put command was not successful:";
	public static final String PUT_SUCCESS_MESSAGE = "The put command was successful.";
	public static final String PUTS_SUCCESS_MESSAGE = "The put and subscirbe command was successful";
	public static final String PUT_UPDATE_MESSAGE = "The tuple was updated successfully.";
	public static final String DELETE_ERROR_MESSAGE = "The tuple was not deleted successfully:";
	public static final String DELETE_SUCCESS_MESSAGE = "The tuple was deleted successfully.";	
	public static final String SERVER_NOT_RESPONSIBLE = "The server is not responsible for this key";
	public static final String SERVER_WRITE_LOCK = "The server is busy with writing operation";
	public static final String SERVER_STOPPED = "Server is stopped";
	public static final String UNSUBSCRIBE_SUCCESS = "The unsubscribe command was successful";
	public static final String UNSUBSCRIBE_NOT_EXIXST = "Key does not exists in the cache of subscribed keys ";
	public static final byte END_OF_MESSAGE = 13;
	
}
