package testing;

import junit.framework.TestCase;

import org.junit.Test;

import app_kvEcs.ECSMock;

public class EnvSetupTest extends TestCase {
    private  static ECSMock ecsService;

    private  int serverNumber ;
    public EnvSetupTest() {
    }

    
    public EnvSetupTest(String serverNumber) {
	this.serverNumber = Integer.parseInt(serverNumber);
    }


    public void setUp() {
    }

    public void tearDown() {
    }
    
    @Test
    public void testOneTimeSetup() throws Exception {
  	System.out.println("--in set up "+serverNumber );
  	ecsService = new ECSMock(serverNumber,System.getProperty("user.dir")
  		+ "/ecs-test.config");
  	assertTrue(ecsService.getECS().getActiveServers().size() + "", !ecsService.getECS()
  		.getActiveServers().isEmpty());
  	Thread.sleep(500);
  	ecsService.getECS().start();
  	Thread.sleep(500);

      }
    
    
    @Test
    public  void testOneTimeTearDown() {
	System.out.println("--shutdown " );
	ecsService.getECS().shutdown();
    }

}
