package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import utilities.LoggingManager;

import client.KVStore;
import common.messages.KVMessage;
import common.messages.ClientMessage;
import common.messages.KVMessage.StatusType;

public class KVClient {

	private KVStore connection; // reference to connection interface
	private Logger logger;

	public KVClient () {
		logger = LoggingManager.getInstance ().createLogger ( this.getClass () );
	}

	/**
	 * The main method that starts the application and interacts with the user
	 * within the defined protocol.
	 */
	public void startApplication () {
		// initialize buffer reader to read user input.
		BufferedReader cons = new BufferedReader ( new InputStreamReader (
				System.in ) );
		logger.debug ( "Input Stream Reader created" );
		// the flag to stop shell interaction
		boolean quit = false;
		while ( ! quit ) {
			System.out.print ( UserFacingMessages.ECHO_PROMPT );
			String input;
			String [] tokens;
			try {
				input = cons.readLine ();
				tokens = input.trim ().split ( UserFacingMessages.SPLIT_ON );
				// user input was split as tokens.
				// safety check
				if ( tokens == null || tokens.length == 0 ) {
					throw new IllegalArgumentException (
							UserFacingMessages.GENERAL_ILLIGAL_ARGUMENT );
				}

				// start parsing the tokens
				KVCommand command = KVCommand.fromString ( tokens [ 0 ] );
				ValidationUtil validationUtil = ValidationUtil.getInstance ();
				switch ( command ) {
				case CONNECT :
					if ( validationUtil.isValidConnectionParams ( tokens ) ) {
						if ( connection == null ) {
							connection = new KVStore ( tokens [ 1 ] ,
									Integer.parseInt ( tokens [ 2 ] ) );
						}
						connection.connect ();
						System.out.println ( "Connected to KV server, "
								+ tokens [ 1 ] + ":" + tokens [ 2 ] );
						logger.info ( "Connected to KV server, " + tokens [ 1 ]
								+ ":" + tokens [ 2 ] );
					}
					break;
				case DISCONNECT :
					connection.disconnect ();
					System.out.println ( "Connection closed." );
					logger.info ( "Connection closed." );
					break;
				case PUT :
					if ( validationUtil.isValidStoreParams ( tokens ) ) {
						KVMessage result = connection.put ( tokens [ 1 ] ,
								tokens [ 2 ] );
						String textResult = handleResponse ( result );
						logger.info ( textResult );
						System.out.println ( textResult );
					}
					break;

				case GET :
					if ( tokens [ 1 ] != null && ! tokens [ 1 ].isEmpty () ) {
						KVMessage result = connection.get ( tokens [ 1 ] );
						String textResult = handleResponse ( result );
						logger.info ( textResult );
						System.out.println ( textResult );
					} else {
						logger.warn ( "Key was not provided." );
						System.out.println ( "Key was not provided." );
					}
					break;
				case LOG_LEVEL :
					if ( validationUtil.isValidLogLevel ( tokens ) ) {
						LoggingManager.getInstance ().setLoggerLevel (
								tokens [ 1 ] );
						logger.info ( "Log Level Set to: " + tokens [ 1 ] );
						System.out
								.println ( "Log Level Set to: " + tokens [ 1 ] );
					}
					break;
				case HELP :
					System.out.println ( UserFacingMessages.HELP_TEXT );
					logger.info ( "Help Text provided to user." );
					break;

				case UN_SUPPORTED :
					System.out
							.println ( UserFacingMessages.UN_SUPPORTED_COMMAND );
					logger.warn ( "User entered unsupported command." );
					break;

				case QUIT :
					quit = true;
					connection.disconnect ();
					System.out.println ( "Quit program based on user request." );
					logger.info ( "Quit program based on user request." );
					break;

				default :
					break;
				}

			} catch ( Exception e ) {
				// report issue to user
				logger.error ( e.getMessage () );
				e.printStackTrace ();

			}

		}

	}

	private String handleResponse ( KVMessage result ) throws IOException {
		String resultText = "";
		switch ( result.getStatus () ) {
		case GET_ERROR :
			resultText = UserFacingMessages.GET_ERROR_MESSAGE
					+ result.getValue ();
			break;
		case GET_SUCCESS :
			resultText = UserFacingMessages.GET_SUCCESS_MESSAGE
					+ result.getValue ();
			break;

		case PUT_ERROR :
			resultText = UserFacingMessages.PUT_ERROR_MESSAGE
					+ result.getValue ();
			break;

		case PUT_SUCCESS :
			resultText = UserFacingMessages.PUT_SUCCESS_MESSAGE;
			break;

		case PUT_UPDATE :
			resultText = UserFacingMessages.PUT_UPDATE_MESSAGE;
			break;

		case DELETE_SUCCESS :
			resultText = UserFacingMessages.DELETE_SUCCESS_MESSAGE;
			break;
		case DELETE_ERROR :
			resultText = UserFacingMessages.DELETE_ERROR_MESSAGE
					+ result.getValue ();
			break;
		case SERVER_NOT_RESPONSIBLE : {		
			resultText = UserFacingMessages.SERVER_NOT_RESPONSIBLE;
			this.connection.updateMetadata ( ( ( ClientMessage ) result )
					.getMetadata () );		
			ClientMessage temp = ( ClientMessage ) connection
					.getLastSentMessage ();
			logger.info ( "re-sent last message " + temp.getStatus () + " : " + temp.getKey ()  );
			if ( temp.getStatus ().equals ( StatusType.PUT ) ) {
				this.connection.put ( temp.getKey () , temp.getValue () );
			} else if ( temp.getStatus ().equals ( StatusType.GET ) ) {
				this.connection.get ( temp.getKey () );
			}
			break;
		}

		case SERVER_STOPPED : {
			resultText = UserFacingMessages.SERVER_STOPPED;
			break;
		}

		case SERVER_WRITE_LOCK : {
			resultText = UserFacingMessages.SERVER_WRITE_LOCK;
			break;
		}

		default :
			resultText = result.getStatus () + result.getKey ()
					+ result.getValue ();
		}

		return resultText;
	}
}
