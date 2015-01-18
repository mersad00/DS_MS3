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
    
    /**
     * this constructor is used for testing application in local 
     * (not SSH)
     * @param: local : just a flag that used for calling this constructor can be any value
     */
    
    public ECSApplication(int numberOfServers,String fileName, int local) {
    	try {
    		logger.debug("<<< WE ARE IN LOCAL WORD NO SSH CALL! Just for Testing Environment!>>>");
    	    this.eCSService = new ECSImpl(numberOfServers,fileName, true);
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
	if(args.length == 3)
		new ECSApplication(Integer.valueOf(args[0]),args[1], 0);
	else
		new ECSApplication(Integer.valueOf(args[0]),args[1]);
    }

}
