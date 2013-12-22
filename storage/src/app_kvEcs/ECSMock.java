package app_kvEcs;

import java.io.FileNotFoundException;

public class ECSMock {

	private ECSImpl ecs;
	public ECSMock (int numberOfServers, String fileName){
		try{
			ecs = new ECSImpl ( numberOfServers , fileName );			
		} catch ( FileNotFoundException e ){
			e.printStackTrace ();
		}
		
	}
	
	public ECSImpl getECS (){
		return this.ecs;
	}
}
