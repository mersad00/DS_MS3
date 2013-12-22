package app_kvEcs;

import java.io.FileNotFoundException;

public class ECSMock {

	private ECSImpl ecs;
	public ECSMock (int numberOfServers){
		try{
			ecs = new ECSImpl ( numberOfServers , "ecs.config" );			
		} catch ( FileNotFoundException e ){
			e.printStackTrace ();
		}
		
	}
	
	public ECSImpl getECS (){
		return this.ecs;
	}
}
