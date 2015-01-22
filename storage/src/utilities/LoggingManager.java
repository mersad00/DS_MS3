package utilities;

import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.*;

public final class LoggingManager {

	private HashSet < Logger > loggerList = new HashSet < Logger > ();
	public static LoggingManager instance = null;

	private LoggingManager () {

	}

	public Logger createLogger ( Class claz ) {
		Logger logger = Logger.getLogger ( claz );
		loggerList.add ( logger );
		return logger;
	}

	public void setLoggerLevel ( String newLevel ) {
		Level level = Level.toLevel ( newLevel );
		Iterator < Logger > itr = loggerList.iterator ();
		while ( itr.hasNext () ) {
			Logger element = itr.next ();
			element.setLevel ( level );
		}
	}
	
	public static LoggingManager getInstance ( ) {
		if ( instance == null ){
			instance = new LoggingManager ();			
		}
		return instance;
	}
}
