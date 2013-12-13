package app_kvClient;

import java.util.Arrays;
import java.util.List;

/**
 * The class contains utility functions that aim to help the shell class in
 * validating user input before executing the commands.
 * 
 * @author Amjad
 * 
 */
public class ValidationUtil {

	private static ValidationUtil VALIDATION_INSTANCE = null;

	/**
	 * A list that contains all the options of log Levels
	 * 
	 */
	public static final List < String > LOG_LEVELS = Arrays.asList ( "ALL" ,
			"DEBUG" , "INFO" , "WARN" , "ERROR" , "FATAL" , "OFF" );

	/**
	 * Singleton provider.
	 * 
	 * @return
	 */
	public static ValidationUtil getInstance () {
		if ( VALIDATION_INSTANCE == null ) {
			VALIDATION_INSTANCE = new ValidationUtil ();
		}
		return VALIDATION_INSTANCE;
	}

	/**
	 * private constructor to avoid class instantiation
	 */
	private ValidationUtil () {
	}

	/**
	 * Validates the IP/host and port input by user. only validates the number
	 * of params, the size of the params and the range of port
	 * 
	 * @param tokens
	 *            the user input
	 * @return true/false status
	 * @throws IllegalArgumentException
	 */
	public boolean isValidConnectionParams ( String [] tokens )
			throws IllegalArgumentException {

		if ( tokens == null ) {
			throw new IllegalArgumentException (
					UserFacingMessages.GENERAL_ILLIGAL_ARGUMENT );
		}

		if ( tokens.length < 3 ) {
			throw new IllegalArgumentException (
					UserFacingMessages.ILLIGAL_PARAM_NUMBER );
		}

		String host = tokens [ 1 ];

		if ( host == null || host.isEmpty () ) {
			throw new IllegalArgumentException ( "Address"
					+ UserFacingMessages.ILLIGAL_PARAM );
		}

		String port = tokens [ 2 ];

		if ( port == null || port.isEmpty () || ! isValidPort ( port ) ) {
			throw new IllegalArgumentException ( "Port"
					+ UserFacingMessages.ILLIGAL_PARAM );
		}

		return true;
	}

	/**
	 * Validates the message to sent input by user. only validates the number of
	 * params, the size of the params
	 * 
	 * @param tokens
	 *            the user input
	 * @return true/false status
	 * @throws IllegalArgumentException
	 */
	public boolean isValidStoreParams ( String [] tokens ) {
		if ( tokens == null ) {
			throw new IllegalArgumentException (
					UserFacingMessages.GENERAL_ILLIGAL_ARGUMENT );
		}

		if ( tokens.length < 3 ) {
			throw new IllegalArgumentException (
					UserFacingMessages.ILLIGAL_PARAM_NUMBER );
		}

		String key = tokens [ 1 ];

		if ( key == null || key.isEmpty () ) {
			throw new IllegalArgumentException ( "key"
					+ UserFacingMessages.ILLIGAL_PARAM );
		}
		
		
		String value = tokens [ 2 ];
		if ( value == null || value.isEmpty () ) {
			throw new IllegalArgumentException ( "value"
					+ UserFacingMessages.ILLIGAL_PARAM );
		}

		return true;
	}

	/**
	 * Validates the log level input by user. only validates the number of
	 * params, the validity of the new log level.
	 * 
	 * @param tokens
	 *            the user input
	 * @return true/false status
	 * @throws IllegalArgumentException
	 */
	public boolean isValidLogLevel ( String [] tokens ) {
		if ( tokens == null ) {
			throw new IllegalArgumentException (
					UserFacingMessages.GENERAL_ILLIGAL_ARGUMENT );
		}

		if ( tokens.length < 2 ) {
			throw new IllegalArgumentException (
					UserFacingMessages.ILLIGAL_PARAM_NUMBER );
		}

		if ( ! LOG_LEVELS.contains ( tokens [ 1 ].toUpperCase () ) ) {
			throw new IllegalArgumentException ( "LogLevel"
					+ UserFacingMessages.ILLIGAL_PARAM );
		}
		return true;
	}

	/**
	 * Validates the port number if in the range of ports
	 * 
	 * @param str
	 * @return
	 */
	private boolean isValidPort ( String str ) {
		try {
			double d = Double.parseDouble ( str );
			if ( d > 65535 ) {// Upper limit of port number
				throw new IllegalArgumentException (
						UserFacingMessages.ILLIGAL_PARAM_PORT );
			}
		} catch ( NumberFormatException nfe ) {
			return false;
		}
		return true;
	}
}
