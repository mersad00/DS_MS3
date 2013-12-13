package app_kvClient;

/**
 * @author AMJAD
 * 
 */

public enum KVCommand {

	CONNECT ( "connect" ) , DISCONNECT ( "disconnect" ) , PUT ( "put" ) , GET ("get"),LOG_LEVEL (
			"logLevel" ) , HELP ( "help" ) , QUIT ( "quit" ) , UN_SUPPORTED (
			"unSupported" );

	private String commandText;

	/**
	 * Enum constructor to initialize the commandText
	 * 
	 * @param commandText
	 */
	private KVCommand ( String commandText ) {
		this.commandText = commandText;
	}

	/**
	 * @return commandText
	 */
	public String getCommandText () {
		return commandText;
	}

	/**
	 * @param commandText
	 * @return EchoClientCommand appropriate <code>enum</code> for the command
	 */
	public static KVCommand fromString ( String commandText ) {
		if ( commandText != null ) {
			for ( KVCommand command : KVCommand.values () ) {
				if ( commandText.equalsIgnoreCase ( command.commandText ) ) {
					return command;
				}
			}
		}
		return KVCommand.UN_SUPPORTED;
	}

}
