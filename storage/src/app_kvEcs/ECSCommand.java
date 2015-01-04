package app_kvEcs;

/**
 * @author AMJAD
 * 
 */

public enum ECSCommand {

    INIT("init"),  START("start"), STOP("stop"), SHUT_DOWN("shutDown"),  SET_WRITE_LOCK("setWriteLock"), MOVE_DATA("moveData"), SEND_METADATA(
	    "sendMetadata"), RELEASE_LOCK("releaseLock"),ACK("ack"), UN_SUPPORTED( "unSupported"), ADD("add"), REMOVE("remove"), LOG_LEVEL(
		    "logLevel"), HELP("help"), QUIT("quit"), REMOVE_DATA("removeData"), REOVER_DATA("recoverData");

    private String commandText;

    /**
     * Enum constructor to initialize the commandText
     * 
     * @param commandText
     */
    private ECSCommand(String commandText) {
	this.commandText = commandText;
    }

    /**
     * @return commandText
     */
    public String getCommandText() {
	return commandText;
    }

    /**
     * @param commandText
     * @return EchoClientCommand appropriate <code>enum</code> for the command
     */
    public static ECSCommand fromString(String commandText) {
	if (commandText != null) {
	    for (ECSCommand command : ECSCommand.values()) {
		if (commandText.equalsIgnoreCase(command.commandText)) {
		    return command;
		}
	    }
	}
	return ECSCommand.UN_SUPPORTED;
    }

}
