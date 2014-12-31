package app_kvEcs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

/**
 * the default consideration is that the ssh private keys and knownHostfiles are in .ssh folder in the home 
 * directory of the user.
 * @author arash
 *
 */


public class SshCaller implements SshInvoker{
	public static Logger logger = Logger.getRootLogger();
	private  static final int port = 22;
	private  String userName = System.getProperty("user.name");
	private  String privateKey = System.getProperty("user.home") + "/.ssh/id_rsa";
	private  String knownHostsfiles = System.getProperty("user.home") + "/.ssh/known_hosts";
	private long timeOut = 3000;
	
	public void setTimeOut(long t){
		this.timeOut = t;
	}
	
	public void setUserName(String s){
		this.userName = s;
	}
	
	public void setPrivatekey(String s){
		this.privateKey = s;
	}
	
	public void setKnownhosts(String s){
		this.knownHostsfiles = s;
	}

	
	public int invokeProcess(String host, String command, String[] arguments){
		
		try{

			JSch jsch = new JSch();
			jsch.setKnownHosts(knownHostsfiles);
			Session session = jsch.getSession(userName, host, port);
			jsch.addIdentity(privateKey);
			session.connect(3000);
			
			// adding the arguments to the command
			for(String argument: arguments)
				command += " " + argument;
			
			Channel channel = session.openChannel("exec");
			((ChannelExec)channel).setCommand(command);
			channel.setInputStream(null);
			
			InputStream in=channel.getInputStream(); 
		      channel.connect();
		      
		    long start = System.currentTimeMillis();
			long end = start + timeOut;
					 
		      String s="";
		      char c;
		      boolean waiting = true;
		      while(System.currentTimeMillis() < end && waiting){
			        while(in.available()>0){
			        	c = (char)in.read();
			        	
			        	/* '\r' indicates that the process on the
			        	 * remote machine started successfully 
			        	 
			        	if(c == '\r'){ 
			        		channel.disconnect();
			        		waiting = false;
			        		break;
			        	}
			        	*/
			        	
			        	s += c;
			        	if(s.contains("$SUCCESS$")){
			        		channel.disconnect();
			        		waiting = false;
			        		break;
			        	}
			        	// the process did not start correctly
			        	if(s.contains("$ERROR$"))
			        		return -1;
			        		
			        	// no more input to read!
			        	if((int)c < 0)
			        		break;
			        }
		        if(channel.isClosed()){
		        	if(in.available() > 0)
		        		continue;
		        	break;
		        }
		      }
		      
		      //logger.info( s + " Process exit-status: "+channel.getExitStatus());
		      channel.disconnect();
		      session.disconnect();
		      
		      if(channel.getExitStatus() == 0){
		    	  logger.info("Remote Server on host: " + host
		    	  		+ " has started! Listening on Port " + arguments[0]);
		    	  return 0;
		      }
		      else
		    	  return -1;
		    }
		    catch(Exception e){	
		      logger.error(e);
		      return -1;
		    }

	}
	
	public int localInvokeProcess(String command, String[] arguments){
		
		ProcessThread thread = new ProcessThread(command, arguments);
		thread.start();
		return 0;
	}
	
	public static void main(String args[]){
		try {
			new LogSetup("logs/ecs/ecs.log", Level.ALL);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	class ProcessThread extends Thread {
		private String command;
		private String [] arguments;
		
		public ProcessThread ( String command, String[] arguments ) {
			// adding the arguments to the command
			this.arguments = arguments;
						for(String argument: arguments)
							command += " " + argument;
						this.command = command;
						//logger.debug("<<<<<<" + command + ">>>>>");						
		}

		public void run () {			
			
			try {
				logger.debug("the arguments are " + arguments[0]);
				logger.debug("the command is : " + command);
				ProcessBuilder pb =
						new ProcessBuilder("java", "-jar","ms3-server.jar", arguments[0],"&");
				String path = System.getProperty("user.dir");
				pb.directory(new File(path));
				
				Process p = Runtime.getRuntime ().exec ( command );	
				InputStream in = p.getInputStream();
				BufferedReader reader = new BufferedReader ( new InputStreamReader(in)  );
				while(reader.readLine ()!= null){
					
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
	}
	}
