package app_kvEcs;

public interface SshInvoker {
	
	/**
	 * sets the UserName for the ssh communication
	 * @param s
	 */
	public void setUserName(String s);
	
	
	/**
	 * sets the Address where the Private key for ssh communication exists
	 * @param s
	 */
	public void setPrivatekey(String s);
	
	
	/**
	 * sets the Address where the KnownHost keys for ssh communication exists
	 * @param s
	 */
	public void setKnownhosts(String s);
	
	/**
	 * sets the time out for the remote process to inform about the success
	 * or failure. The default value is 3 seconds
	 * @param miliseconds
	 */
	public void setTimeOut(long miliseconds);
	
	/**
	 * invokes the KVserver Process
	 * @param host: The address of the remote host
	 * @param serverPort: The port of the remote host
	 * @param cacheSize
	 * @param cacheStr
	 * @param ecsPort : give the Kvserver the Port for communication with ECS
	 @return: 0 in case of Success and -1 in case of Error
	 */
	public int invokeProcess(String host,String command,String[]arguments);
}