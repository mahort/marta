package marta.taxa;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.Logger;

public final class TaxaLogger {

	private static SimpleDateFormat fmt;

	public synchronized static String getTime(){
		fmt = new SimpleDateFormat();
		fmt.setCalendar(Calendar.getInstance());
		fmt.applyPattern("MMddyy-HHmmss");
		return fmt.format( fmt.getCalendar().getTime());
	}

	public synchronized static void logDebug( Logger logger, String debug, String time, String shortid ){
		logger.debug( new StringBuffer(( shortid == null ? "" : "Id: ".concat( shortid ))).append( " time: " ).append( 
				time ).append( " " ).append( debug ).toString());
	}

	public synchronized static void logError( Logger logger, String error, String time, String shortid ){
		logger.error( new StringBuffer(( shortid == null ? "" : "Id: ".concat( shortid ))).append( " time: " ).append( 
				time ).append( " " ).append( error ).toString());
	}

	public synchronized static void logInfo( Logger logger, String info, String time, String shortid ){
		logger.info( new StringBuffer(( shortid == null ? "" : "Id: ".concat( shortid ))).append( " time: " ).append( 
				time ).append( " " ).append( info ).toString());
	}

	public static void updateVotingHistory( Logger logger, String outputdir, String groupId, String options ){

		BufferedWriter writer = null;

		try{

	        writer = new BufferedWriter(
	        		new OutputStreamWriter( new FileOutputStream( 
	        				new StringBuffer( outputdir ).append( "/history.txt" ).toString(), true ), "UTF-8"));

	        writer.write("----------------------------------------------------------------\n");
	        writer.write("groupId: ");
	        writer.write(groupId);
	        writer.write('\n');
	        writer.write( options );
	        writer.write('\n');
	        writer.flush();
	        writer.close();

		} catch( IOException ioEx ){
			ioEx.printStackTrace(System.err);
			logger.error( "Error while recording the history.");
			logger.error( ioEx.getMessage());

		} finally {}
	}
}
