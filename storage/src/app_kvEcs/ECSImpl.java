package app_kvEcs;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import org.apache.log4j.Logger;

import utilities.LoggingManager;
import common.ServerInfo;

public class ECSImpl implements ECS {

    private List<ServerInfo> serverRepository;
    Logger logger = LoggingManager.getInstance().createLogger(this.getClass());

    /**
     * @param fileName
     *            : the name of the configuration file
     * @throws FileNotFoundException
     */
    public ECSImpl(String fileName) throws FileNotFoundException {
	// parse the server repository
	readServerInfo(fileName);
	initService(pickRandomValue(serverRepository.size()));
    }

    private void readServerInfo(String fileName) throws FileNotFoundException {
	Scanner fileReader = new Scanner(new File(fileName));
	fileReader.useDelimiter("\n");
	serverRepository = new ArrayList<ServerInfo>();

	while (fileReader.hasNext()) {
	    serverRepository.add(new ServerInfo(fileReader.next().trim()));
	}
	fileReader.close();

    }

    /** Generates a random number in range
     * @param size: the range upper bound
     * @return
     */
    private int pickRandomValue(int size) {
	Random randomGenerator = new Random();
	int randomNumber = randomGenerator.nextInt(size);

	logger.info("Picked "+randomNumber+" as the inital count to start nodes.");
	return randomNumber;
    }
    
    

    @Override
    public boolean removeNode() {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public void initService(int numberOfNodes) {
	

    }

    @Override
    public void start() {
	// TODO Auto-generated method stub

    }

    @Override
    public void shutdown() {
	// TODO Auto-generated method stub

    }

    @Override
    public void addNode() {
	// TODO Auto-generated method stub

    }

    @Override
    public void stop() {
	// TODO Auto-generated method stub

    }

}
