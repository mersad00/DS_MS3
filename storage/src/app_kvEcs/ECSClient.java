package app_kvEcs;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import utilities.LoggingManager;
import app_kvClient.KVCommand;
import app_kvClient.UserFacingMessages;
import app_kvClient.ValidationUtil;
import client.KVStore;

import common.messages.KVMessage;

public class ECSClient {

    private ECS eCSService;
    private Logger logger;

    public ECSClient(ECS eCSService) {
	logger = LoggingManager.getInstance().createLogger(this.getClass());
	seteCSService(eCSService);
    }

    public ECS geteCSService() {
        return eCSService;
    }

    public void seteCSService(ECS eCSService) {
        this.eCSService = eCSService;
    }

    /**
     * The main method that starts the ecs admin interface interacts with the user
     * within the defined protocol.
     */
    public void startApplication() {
	// initialize buffer reader to read user input.
	BufferedReader cons = new BufferedReader(new InputStreamReader(
		System.in));
	logger.debug("ECS Input Stream Reader created");
	// the flag to stop shell interaction
	boolean quit = false;
	while (!quit) {
	    System.out.print(UserFacingMessages.ECS_ECHO_PROMPT);
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
		}

		// start parsing the tokens
		ECSCommand command = ECSCommand.fromString(tokens[0]);
		ValidationUtil validationUtil = ValidationUtil.getInstance();
		switch (command) {
		case START:
			eCSService.start();
//			System.out.println("Connected to KV server, "
//				+ tokens[1] + ":" + tokens[2]);
//			logger.info("Connected to KV server, " + tokens[1]
//				+ ":" + tokens[2]);
		    break;
		case STOP:
		    eCSService.stop();
//		    System.out.println("Connection closed.");
		    //		    logger.info("Connection closed.");
		    break;
		case ADD:
		    eCSService.addNode();
//		    logger.info(textResult);
//		    System.out.println(textResult);
		    break;

		case REMOVE:
		    eCSService.removeNode();
//			logger.info(textResult);
//			System.out.println(textResult);
//			logger.warn("Key was not provided.");
//			System.out.println("Key was not provided.");
		    break;
		case LOG_LEVEL:
		    if (validationUtil.isValidLogLevel(tokens)) {
			LoggingManager.getInstance().setLoggerLevel(tokens[1]);
			logger.info("Log Level Set to: " + tokens[1]);
			System.out.println("Log Level Set to: " + tokens[1]);
		    }
		    break;
		case HELP:
		    System.out.println(UserFacingMessages.HELP_TEXT);
		    logger.info("Help Text provided to user.");
		    break;

		case UN_SUPPORTED:
		    System.out.println(UserFacingMessages.UN_SUPPORTED_COMMAND);
		    logger.warn("User entered unsupported command.");
		    break;

		case QUIT:
		    quit = true;
//		    connection.shut();
		    System.out.println("Quit program based on user request.");
		    logger.info("Quit program based on user request.");
		    break;

		default:
		    break;
		}

	    } catch (Exception e) {
		// report issue to user
		logger.error(e.getMessage());

	    }

	}

    }

}
