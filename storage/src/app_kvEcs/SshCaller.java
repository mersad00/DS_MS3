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
		
		try{
			// adding the arguments to the command
			for(String argument: arguments)
				command += " " + argument;
			//logger.debug("<<<<<<" + command + ">>>>>");
			ProcessBuilder pb =
					new ProcessBuilder("nohup","java", "-jar","ms3-server.jar", arguments[0],"&");
			String path = System.getProperty("user.dir");
			pb.directory(new File(path));
			
			Process p = pb.start();
			InputStream in = p.getInputStream();
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
			        		waiting = false;
			        		break;
			        	}
			        	
			        	*/
			        	
			        	s += c;
			        	
			        	if(s.contains("$SUCCESS$")){
			        		s.replace("$SUCCESS$", "");
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
		        
		      }
		      logger.info("Local Server" 
		    	  		+ " has started! Listening on Port " + arguments[0]);
		    	 
		    return 0;
		    }
		    catch(Exception e){	
		      logger.error(e);
		      return -1;
		    }
	}
	
	public static void main(String args[]){
		try {
			new LogSetup("logs/ecs/ecs.log", Level.ALL);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	}