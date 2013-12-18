package app_kvEcs;

/**
 * ECS: External configuration service, the service that controls the storgae
 * server
 * 
 * @author Amjad
 * 
 */
public interface ECS {

    /** Send an SSH call to launch number of servers, then send an INIT message to hand meta data.
     * @param numberOfNodes
     */
    public void initService(int numberOfNodes);

    /**
     * Is called by the ECS client to start all the services 
     */
    public void start();
    
    /**
     * Is called by the ECS client to stop all the services 
     */
    public void stop();

    /**
     * Is called by the ECS client to shut down all the services 
     */
    public void shutdown();

    /**
     * Adds a node to the ring and performs the protocol to update the meta data, and move data operations
     */
    public void addNode();

    /**
     * Removes a node to the ring and performs the protocol to update the meta data, and move data operations
     */
    public boolean removeNode();

}
