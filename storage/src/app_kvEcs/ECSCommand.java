package app_kvEcs;

/**
 * @author AMJAD
 * 
 */

public enum ECSCommand {

	START ( "start" ) , STOP ( "stop" ) , ADD ( "add" ) , REMOVE ("remove"),LOG_LEVEL (
			"logLevel" ) , HELP ( "help" ) , QUIT ( "quit" ) , SHUT_DOWN("shutDown"),UN_SUPPORTED (
			"unSupported" );

	private String commandText;

	/**
	 * Enum constructor to initialize the commandText
	 * 
	 * @param commandText
	 */
	private ECSCommand ( String commandText ) {
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
	public static ECSCommand fromString ( String commandText ) {
		if ( commandText != null ) {
			for ( ECSCommand command : ECSCommand.values () ) {
				if ( commandText.equalsIgnoreCase ( command.commandText ) ) {
					return command;
				}
			}
		}
		return ECSCommand.UN_SUPPORTED;
	}

}
