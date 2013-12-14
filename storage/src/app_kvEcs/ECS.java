package app_kvEcs;

/**
 * ECS: External configuration service, the service that controls the storgae
 * server
 * 
 * @author Amjad
 * 
 */
public interface ECS {

    public void initService(int numberOfNodes);

    public void start();
    
    public void stop();

    public void shutdown();

    public void addNode();

    public boolean removeNode();

}
