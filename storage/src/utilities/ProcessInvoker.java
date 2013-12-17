package utilities;


import java.util.List;

import org.apache.log4j.Logger;

import common.ServerInfo;



public class ProcessInvoker {
    
    Logger logger = LoggingManager.getInstance().createLogger(this.getClass());
    private static final String[] commandArrayWindows ={"ssh","-n","host","java","-jar","/ser.jar ","port"};
    private static final String[] commandArrayLinux ={"ssh","-n","host","nohup","java","-jar","/ser.jar ","port"};
    private String[] commandArray;
    
    public ProcessInvoker() {
	
	if (OsUtils.isWindows()){
	    commandArray = commandArrayWindows;
	}else{
	    commandArray = commandArrayLinux;
	}
	
    }

    public void ivnokeCommands(List<ServerInfo> serverDetails) {

	for (ServerInfo server : serverDetails){
	    try {
		// create a new array of 2 strings
		String[] command = getCommandArray(server);

		// create a process and execute cmdArray and currect environment
		Process process = Runtime.getRuntime().exec(command,null);
		
		server.setServerLaunched(true);

	    }  catch (Exception ex) {
		ex.printStackTrace();
	    }
	}
    }

    private String[] getCommandArray(ServerInfo server) {
	// based on the OS, the command array is determined, because the nohup command doesn't work on windows
	String[] command = new String[commandArray.length];
	System.arraycopy(commandArray, 0, command, 0, commandArray.length);
	for (int i = 0; i < command.length; i++) {
	    if (command[i].equals("host")){
		command[i] = server.getAddress();
	    }else  if (command[i].equals("port")){
		command[i] = ""+server.getPort();
	    }

	}

	logger.debug("Prepared command with the following params"+command);

	return command;
    }
    
    
    
}
