package app_kvClient;

/**
 * @author Amjad 
 * The class contains all the user facing strings of the application.
 */
public class UserFacingMessages {

	
	public static final String ECHO_PROMPT = "KVClient> ";
	
	public static final String SPLIT_ON = "\\s+";
	
	public static final String HELP_TEXT = "KVClient:  The second phase of distributed systems[IN 2259] course project."
			+ "\nUsage:"
			+ "\nConnect <address> <port>: Tries to establish a TCP- connection to the KV server based on the given server address and the port number of the echo service.\n"
			+ "address: Hostname or IP address of the KV server.\nport: The port of the KV service on the respective server.\n"
			+ " Example: connect 192.168.50.1 50000 \n"
			+ "disconnect: Tries to disconnect from the connected server."
			+ "\nput <key> <value>: Inserts a key-value pair into the storage server data structure."
			+ "\nUpdates (overwrites) the current value with the given value if the server already contains the specified key."
			+ "\nDeletes the entry for the given key if <value> equals null. "
			+ "\nlogLevel <level>: Sets the logger to the specified log level."
			+ "\nlevel: One of the following log4j log levels: (ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF)."
			+ "\nHelp: Prints the help and usage tips."
			+ "\nquit: Tears down the active connection to the server and exits the program execution.";
	
	public static final String GENERAL_ILLIGAL_ARGUMENT = "Please enter a valid command. \n"+ HELP_TEXT;
	
	public static final String ILLIGAL_PARAM_NUMBER = "The number of the command parameters is not as expected, Please use the help command to see an example.";
	
	public static final String ILLIGAL_PARAM = " is not as expected, Please use the help command to see an example.";
	
	public static final String ILLIGAL_PARAM_PORT = "Port number in invalid, Please use the help command to see an example.";
	
	public static final String UN_SUPPORTED_COMMAND = "Unknown command.\n"+ HELP_TEXT;
	
	public static final String GET_ERROR_MESSAGE = "The get command was not successful:";
	
	public static final String GET_SUCCESS_MESSAGE = "The get command was successful, The value received:";
	public static final String PUT_ERROR_MESSAGE = "The put command was not successful:";
	public static final String PUT_SUCCESS_MESSAGE = "The put command was successful.";
	public static final String PUT_UPDATE_MESSAGE = "The tuple was updated successfully.";
	public static final String DELETE_ERROR_MESSAGE = "The tuple wasnot deleted successfully:";
	public static final String DELETE_SUCCESS_MESSAGE = "The tuple was deleted successfully.";
	
	public static final byte END_OF_MESSAGE = 13;
	
}
