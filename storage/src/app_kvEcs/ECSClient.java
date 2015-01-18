package app_kvEcs;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import utilities.LoggingManager;
import app_kvClient.UserFacingMessages;
import app_kvClient.ValidationUtil;

/**
 * This UI Class 
 *
 */

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
		    break;
		case STOP:
		    eCSService.stop();
		    break;
		case ADD:
		    eCSService.addNode();
		    break;

		case REMOVE:
		    eCSService.removeNode();
		    break;
		    
		case SHUT_DOWN:{
		    eCSService.shutdown();
		}
		    break;
		case LOG_LEVEL:
		    if (validationUtil.isValidLogLevel(tokens)) {
			LoggingManager.getInstance().setLoggerLevel(tokens[1]);
			logger.info("Log Level Set to: " + tokens[1]);
			System.out.println("Log Level Set to: " + tokens[1]);
		    }
		    break;
		case HELP:
		    System.out.println(UserFacingMessages.ECS_HELP_TEXT);
		    logger.info("Help Text provided to user.");
		    break;

		case UN_SUPPORTED:
		    System.out.println(UserFacingMessages.UN_SUPPORTED_COMMAND);
		    logger.warn("User entered unsupported command.");
		    break;

		case QUIT:
		    quit = true;
		    eCSService.shutdown();
		    System.out.println("Quit program based on user request.");
		    logger.info("Quit program based on user request.");
		    System.exit(1);
		    break;

		default:
		    break;
		}

	    } catch (Exception e) {
		// report issue to user
		e.printStackTrace();
		logger.error(e.getMessage());

	    }

	}

    }

}
