package threads;

import java.io.IOException;

import app_kvServer.KVServer;

public class ServerThread extends Thread {
	
	
	private KVServer server;
	
	public ServerThread ( KVServer server ){
		this.server = server;
	}
	
	
	public void run(){
		try {
			this.server.startServer ();
		} catch ( IOException e ) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
