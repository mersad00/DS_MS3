package app_kvEcs;

import java.io.FileNotFoundException;
import java.io.IOException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import utilities.LoggingManager;

/**
 * This class is the entry point for the ECS. It creates the service and the
 * client
 * 
 * @author Amjad
 * 
 */
public class ECSApplication {

    /**
     * The ECS functionality library
     */
    private ECS eCSService;

    /**
     * ECS command line admin interface
     */
    private ECSClient eCSClient;

    private Logger logger = LoggingManager.getInstance ().createLogger ( this.getClass () );
    
    
    public ECSApplication(int numberOfServers,String fileName) {
	try {
	    this.eCSService = new ECSImpl(numberOfServers,fileName);
	    this.eCSClient = new ECSClient(eCSService);
	    startCSClient();
	} catch (FileNotFoundException e) {
	    logger.error("ECS couldn't be started because config file was not found.");
	    System.exit(1);
	}

    }

    public ECSApplication(ECS eCSService, ECSClient eCSClient) {
	this.eCSService = eCSService;
	this.eCSClient = eCSClient;
    }

    private void startCSClient() {
	this.eCSClient.startApplication();
    }

    /**
     * @param args
     */
    public static void main(String[] args) {

	try {
	    new LogSetup("logs/ecs/ecs.log", Level.ALL);
	} catch (IOException e) {
	    e.printStackTrace();
	}
	new ECSApplication(Integer.valueOf(args[0]),args[1]);
    }

}
