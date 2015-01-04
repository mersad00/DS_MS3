package app_kvServer;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import app_kvEcs.FailureMessage;
import utilities.LoggingManager;

public class HeartbeatThread extends Thread {
	static Logger logger;
	KVServer parent;
	/*
	 * TimerTask timerTask; Timer timer;
	 */
	boolean running;

	HeartbeatThread(KVServer server) {
		logger = LoggingManager.getInstance().createLogger(this.getClass());
		this.parent = server;
		running = true;
		/*
		 * timer = new Timer(); running = true; timerTask = new TimerTask() {
		 * 
		 * @Override public void run() {
		 * logger.debug("begin heartbeat timer run..." +
		 * parent.getThisServerInfo()); try { // /send heart beats
		 * HeartbeatMessage msg = new HeartbeatMessage();
		 * msg.setCoordinatorServer(parent.getThisServerInfo()); // /send to
		 * both replicas the heart beat message parent.sendHeartbeatMessage(msg,
		 * parent.getThisServerInfo() .getFirstReplicaInfo());
		 * parent.sendHeartbeatMessage(msg, parent.getThisServerInfo()
		 * .getSecondReplicaInfo()); } catch (Exception exc) {
		 * logger.error("error in timer..."); }
		 * logger.debug("end heartbeat timer run..."); } };
		 */

	}

	public void stopTicking() {
		/*
		 * timer.cancel(); timer.purge();
		 */
		running = false;
	}

	@Override
	public void run() {
		// logger.debug("Heartbeat(" + parent.getThisServerInfo().getPort()
		// + ")started!");
		while (running) {
			try {
				// /send heart beats
				HeartbeatMessage msg = new HeartbeatMessage();
				msg.setCoordinatorServer(parent.getThisServerInfo());
				// /send to both replicas the heart beat message
				if (running
						&& !parent.getThisServerInfo().getFirstReplicaInfo()
								.equals(parent.getThisServerInfo())) {
					parent.sendHeartbeatMessage(msg, parent.getThisServerInfo()
							.getFirstReplicaInfo());
				}
				if (running
						&& !parent.getThisServerInfo().getSecondReplicaInfo()
								.equals(parent.getThisServerInfo())) {
					parent.sendHeartbeatMessage(msg, parent.getThisServerInfo()
							.getSecondReplicaInfo());
				}

				Thread.sleep(10000);

				logger.debug("Eval Heartbeat("
						+ parent.getThisServerInfo().getPort()
						+ ") [lastseens: "
						+ parent.getThisServerInfo().getFirstCoordinatorInfo()
								.getPort()
						+ ":"
						+ parent.getFirstCoordinatorLastSeen()
						+ ","
						+ parent.getThisServerInfo().getSecondCoordinatorInfo()
								.getPort() + ":"
						+ parent.getSecondCoordinatorLastSeen() + "]");

				if (parent.getFirstCoordinatorLastSeen() != null) {
					long minutes = getMinutesDiffNow(parent
							.getFirstCoordinatorLastSeen());
					if (minutes > 1) {
						logger.debug("Heartbeat("
								+ parent.getThisServerInfo().getPort()
								+ ") Detected Failure["
								+ parent.getThisServerInfo()
										.getFirstCoordinatorInfo().getPort()
								+ "]");
						
						///send fail message to ECS
						FailureMessage failmsg = new FailureMessage();
						failmsg.setFailedServer(parent.getThisServerInfo()
										.getFirstCoordinatorInfo());
						parent.sendFailureMessage(failmsg);
					}
				}
				if (parent.getSecondCoordinatorLastSeen() != null) {
					long minutes = getMinutesDiffNow(parent
							.getSecondCoordinatorLastSeen());
					if (minutes > 1) {
						logger.debug("Heartbeat("
								+ parent.getThisServerInfo().getPort()
								+ ") Detected Failure["
								+ parent.getThisServerInfo()
										.getSecondCoordinatorInfo().getPort()
								+ "]");
						///send fail message to ECS
						FailureMessage failmsg = new FailureMessage();
						failmsg.setFailedServer(parent.getThisServerInfo()
										.getSecondCoordinatorInfo());
						parent.sendFailureMessage(failmsg);
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		logger.debug("Heartbeat(" + parent.getThisServerInfo().getPort()
				+ ") Stopped");
	}

	private long getMinutesDiffNow(Date value) {
		Date now = new Date();
		long diff = now.getTime() - value.getTime();
		long minutes = TimeUnit.MILLISECONDS.toMinutes(diff);
		return minutes;
	}
}