/*
 * by Matt Horton
 * 
 * AssignTaxa is the 'main' application of the software bundle 'marta'. Through AssignTaxa one 
 * taxonomically classifies each sequence in a tab delimited file (containing an id and sequence to BLAST.
 * 
 * AssignTaxa creates 'jobs' using the Quartz utility. Each job is responsible for its own sequence.
 * To change voting parameters for a set of sequences that have already been investigated, see the Revote class.
 */

package marta.taxa;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FilenameFilter;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.Logger;
import org.apache.log4j.FileAppender;
import org.quartz.SimpleTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;

public class AssignTaxa {
	
	static Logger log4j = Logger.getLogger("marta.taxa");

	private static int[] dim( String filename, char delim ) throws IOException {

		Reader reader = new InputStreamReader( new FileInputStream( filename ));        

        char[] buffer = new char[4096];
        int line = 0, field = 0;

        for( int read = reader.read( buffer ); read >= 0; read = reader.read( buffer )){

            for( int i = 0; i < read ; i++){
                if(( line == 0 ) && ( buffer[i] == delim )){
                	field++;

                } else if( buffer[i] == '\n' ){
                	line++;
                }
            }

        } field++; // get the last field, since counting the delimiters only gives us the (n-1) separators.

        reader.close();
		System.gc();

        return new int[]{line,field};
	}

	public static void main(String[] args){

		boolean forked = false, minscore = false, retries = false;
		boolean resolveCultured = true;
		boolean qsubMemoryRequirement = false, parallel = false;

		double percentile = 98;
		int tophits = 100, wordsize = 28, cores = 3;
		int forkSize = 5000;
		int percentageIdentity = 97;

		String blastfile = null; 
		String cutoffs = "1,1,1,1,1,1,2/3,2/3";
		String groupId = null, host = null, ncbiDatabase = "nt", outputdir = "~/"; // TODO:: GET THE HEAD DIR

		if( args.length > 0 ){
			if( args[0].equals("--examples")){
				System.out.println("example usage (all opts):");
				System.out.println("java -Xmx1028m -Xms256m -cp ~args~ marta.taxa.AssignTaxa file -out= -tile= -co= -minscore -parallel -qsubmem -recall -noresolve -cutoffs= -group=");
				System.exit(0);
			}

			blastfile = args[0];
			outputdir = blastfile.substring(0,blastfile.lastIndexOf("/"));			

			StringBuffer options = new StringBuffer();
			for( int i = 1; i < args.length; i++ ){
				options.append(args[i]).append(" ");

			} options.append(" "); // one terminal space to match on for the last option.

			String arg = options.toString(), tmp;
//
// booleans
			forked = ( arg.indexOf("-forked") != -1 ); // forked is internal... called by the qsub scripts below and hidden from the user.
			minscore = ( arg.indexOf("-minscore") != -1 );
			parallel = ( arg.indexOf("-parallel") != -1 );
			qsubMemoryRequirement = ( arg.indexOf("-qsubmem") != -1);
			resolveCultured = ( arg.indexOf("-noresolve") == -1 );
			retries = ( arg.indexOf("-retries") != -1 );

			if( arg.indexOf("-top=") != -1){
				tmp = arg.substring(arg.indexOf("-top=") + "-top=".length(), arg.indexOf(" ",arg.indexOf("-top=")));
				tophits = Integer.parseInt(tmp);
			}

			if( arg.indexOf("-db=") != -1){
				ncbiDatabase = arg.substring(arg.indexOf("-db=") + "-db=".length(), arg.indexOf(" ",arg.indexOf("-db=")));
			}

			if( arg.indexOf("-forksize=") != -1){
				tmp = arg.substring(arg.indexOf("-forksize=") + "-forksize=".length(), arg.indexOf(" ",arg.indexOf("-forksize=")));
				forkSize = Integer.parseInt(tmp);
			}

			if( arg.indexOf("-co=") != -1){
				tmp = arg.substring(arg.indexOf("-co=") + "-co=".length(), arg.indexOf(" ",arg.indexOf("-co=")));
				cores = Integer.parseInt(tmp);
			}

			if( arg.indexOf("-tile=") != -1){
				tmp = arg.substring(arg.indexOf("-tile=") + "-tile=".length(), arg.indexOf(" ",arg.indexOf("-tile=")));
				percentile = Double.parseDouble(tmp);
			}

			if( arg.indexOf("-group=") != -1 ){
				groupId = arg.substring(arg.indexOf("-group=") + "-group=".length(), arg.indexOf(" ",arg.indexOf("-group=")));
			}

			if( arg.indexOf("-host=") != -1 ){
				host = arg.substring(arg.indexOf("-host=") + "-host=".length(), arg.indexOf(" ",arg.indexOf("-host=")));
			}

			if( arg.indexOf("-ws=") != -1){
				tmp = arg.substring(arg.indexOf("-ws=") + "-ws=".length(), arg.indexOf(" ",arg.indexOf("-ws=")));
				wordsize = Integer.parseInt(tmp);
			}

			if( arg.indexOf("-cutoffs=") != -1 ){
				cutoffs = arg.substring(arg.indexOf("-cutoffs=") + "-cutoffs=".length(), arg.indexOf(" ",arg.indexOf("-cutoffs=")));
			}

			if( arg.indexOf("-p=" ) != -1 ){
				percentageIdentity = Integer.parseInt( 
						arg.substring(arg.indexOf("-p=") + "-p=".length(), arg.indexOf(" ",arg.indexOf("-p="))));
			}

			if( arg.indexOf("-out=") != -1){
				outputdir = arg.substring(arg.indexOf("-out=") + "-out=".length(), arg.indexOf(" ",arg.indexOf("-out=")));

				java.io.File f = new java.io.File(outputdir);
				if( !f.exists()){
					System.err.println("Output directory: "+ outputdir + " does not exist");
					System.err.println("-------------------------------------------------");
					System.exit(1);
				}
			}

		} else {
			System.err.println("Usage: marta.taxa.AssignTaxa blastfile [optional args, e.g. tophits or different outputdir]");
			System.exit(1);
		}

//
// configure log4j
		if( forked ){
			FileAppender appender = (FileAppender)log4j.getAppender("myAppender");

			if(( appender != null ) && ( appender.getFile() != null )){
				String logger = new StringBuffer(
						( appender.getFile().indexOf("/") != -1 ? appender.getFile().substring( appender.getFile().lastIndexOf("/") + 1 ) : appender.getFile().trim())).toString();
				logger = outputdir.concat("_").concat( host ).concat(".cre");

				log4j.info("Redirecting output to: " + logger );

				appender.setFile( logger );
				appender.activateOptions();

			} else {
				log4j.error( "Unable to locate the fileappender 'myAppender'. Please review log4j.properties file.");
			}
		}

		log4j.info("Using source file: " + blastfile );
		log4j.info("Using output directory: " + outputdir );
		log4j.info("Running AssignTaxa with minscore option set to: " + minscore );

// parse the file, assuming that it is in the following format:
// first column: sequence-name
// second column: sequence

		BufferedReader reader = null;

		try{
			int[] dimensions = dim( blastfile, '\t');
			if(dimensions[1] != 2 ){
				log4j.error("Expect a 2 column file with format: sequence-name\tsequence");
				System.err.println("Wrong file format for blastfile: "+ blastfile);
				System.exit(1);
			}

			reader = new BufferedReader( new InputStreamReader( new FileInputStream( blastfile ), "UTF-8" ));

			String[][] sequences = new String[dimensions[0]][2];
			String line; int lineNumber = 0;
			while(( line = reader.readLine()) != null ){
				sequences[lineNumber++] = line.split("\t");
			}

			if( !forked ){
				StringBuffer options = new StringBuffer();
				options.append( "output-dir: " ).append( outputdir ).append('\n');
				options.append( "source-file: " ).append( blastfile ).append('\n');
				options.append( "fork-size: " ).append( forkSize ).append('\n');
				options.append( "minscore: " ).append( minscore ).append('\n');
				
				if( !minscore ){
					options.append( "percentile: " ).append( String.valueOf( percentile )).append('\n');

				} else {
					options.append( "resolve-cultured: " ).append( String.valueOf( resolveCultured )).append('\n');
				}

				options.append( "word-size: " ).append( String.valueOf( wordsize )).append('\n');
				options.append( "percent-identity: " ).append( percentageIdentity ).append('\n');

				options.append( "Number of top-hits: " ).append( String.valueOf( tophits )).append('\n');
				options.append( "Thresholds per rank: " ).append( String.valueOf( cutoffs )).append('\n');
				
				TaxaLogger.updateVotingHistory( log4j, outputdir, ( groupId == null ? "default" : groupId ), options.toString());
			}

			if( parallel ){
//
// split the file up and recall with qsub.
				boolean success = runBlastParallel( outputdir, sequences, ncbiDatabase, 
									forkSize, minscore, cores, groupId, cutoffs, resolveCultured, 
									percentile, wordsize, tophits, retries );

				if( !success ){
					System.err.println("Unable to partition the blast file into smaller partitions for cluster-sequencing");
					System.exit(1);
				}

				success = putBlastJobsOnQueue( outputdir, qsubMemoryRequirement, retries );
				if( !success ){
					System.err.println("Unable to queue the blast jobs.");
					System.exit(1);
				}

				return;
			}

			SchedulerFactory schedFact = new org.quartz.impl.StdSchedulerFactory();
			Scheduler sched = schedFact.getScheduler();
			sched.start();

			SimpleTrigger trigger;

			StringBuffer base = new StringBuffer( " -d " ).append( ncbiDatabase ).append( " -I T -v " ).append(
					String.valueOf( tophits )).append( " -b " ).append( String.valueOf(tophits)).append(
					" -a " ).append( cores ).append( " -W " ).append(  
					wordsize ).append( " -m 8 -p " ).append( String.valueOf( percentageIdentity ));

			log4j.info("Basic blast query: " + base.toString());
			long startTime;

			for( int m = 0; m < sequences.length; m++ ){

				JobDetail blastJob = new JobDetail( sequences[m][0], null, AssignTaxaJob.class );

				blastJob.getJobDataMap().put("scorereq", String.valueOf( minscore ));	// whether or not to always align in muscle to find minimum dist.
				blastJob.getJobDataMap().put("cmdline", base.toString());						// cmd-line to launch blastall.
				blastJob.getJobDataMap().put("id", sequences[m][0]);							// short-id assigned by user..
				blastJob.getJobDataMap().put("sequence", sequences[m][1]); 						// target
				blastJob.getJobDataMap().put("outputdir", outputdir );							// destination directory chosen by user.
				blastJob.getJobDataMap().put("thresholds", String.valueOf(cutoffs));
				blastJob.getJobDataMap().put("resolvecultureds", String.valueOf(resolveCultured));
				blastJob.getJobDataMap().put("percentile", String.valueOf( percentile ));
				blastJob.getJobDataMap().put( "groupid", groupId );

				startTime = System.currentTimeMillis() + 1000; // use the current time + 1 sec.
				trigger = new SimpleTrigger(new StringBuffer("t").append(sequences[m][0]).toString(),
												null, // I am not providing a 'group' name...
												new java.util.Date(startTime), // this is my start time...
												null, 0, 0L); // no endtime, repeatcount, repeatinterval provided for 1x

				sched.scheduleJob( blastJob, trigger );

				if(( parallel & m %100 == 0 ) | ( !parallel & m % 1000 == 0 )){
					log4j.info( "Progress: " + (double)m/sequences.length);
				}
			}

//
// try to shutdown the scheduler cleanly.

			Thread.sleep(20000);
			boolean confirmedOnce = false, confirmedTwice = false;
			do{

				if( sched.getCurrentlyExecutingJobs().size() == 0 ){
					if( !confirmedOnce ){
						confirmedOnce = true;

					} else if( !confirmedTwice ){
						confirmedTwice = true;

					} else {
						log4j.info("No more jobs to fire (getcurrent==0). Exiting now.");
						break;
					}

				} else if( confirmedOnce ){
					confirmedOnce = false;
					confirmedTwice = false;
				}

				Thread.sleep(30000); // ask every 60 seconds.

			} while( true );

			sched.shutdown();

			return;

		} catch( IOException ioEx ){
			log4j.error( "Error occurred at "+ getTime());
			ioEx.printStackTrace(System.err);
			log4j.error("IOException in AssignTaxa");

		} catch( Exception ex ){
			log4j.error( "Error occurred at "+ getTime());
			ex.printStackTrace();
			ex.printStackTrace(System.err);
			log4j.error("Ex message:  " + ex.getMessage());
			
		} finally {}

		return;
	}

	private static boolean runBlastParallel( String outputdir, String[][] sequences, String ncbiDatabase, int queuesize, boolean minscore, 
			int cores, String groupId, String cutoffs, boolean resolveCultured, double slippageTolerance, int wordsize, int tophits, boolean retries ){
//
// this method is responsible for splitting the files up -and-
// firing the job for queue-submission...
		BufferedWriter writer = null;

		try{

// iterate through, writing the file of set size.
			int partition = 0;

			for( int i = 0; i < sequences.length; i++ ){

				if( i == 0 ){
					writer = new BufferedWriter( new OutputStreamWriter( new FileOutputStream( 
							new StringBuffer( outputdir ).append( "/blajob" ).append(partition).toString(), false ), "UTF-8"));

				} else if( i % queuesize == 0 ){

//
// wrap up the current blast.fork file.
					writer.flush();
					writer.close();

					partition++;

					writer = new BufferedWriter( new OutputStreamWriter( new FileOutputStream( 
							new StringBuffer( outputdir ).append( "/blajob" ).append(partition).toString(), false ), "UTF-8"));
				}

				writer.write( sequences[i][0]);
				writer.write( '\t' );
				writer.write( sequences[i][1]);
				writer.write( '\n' );
			}

			for( int i = 0; i <= partition; i++ ){

				String fork = new StringBuffer( outputdir ).append( "/FORK" ).append( i ).toString();

				// make the output directory...
				boolean success = (new File( fork )).mkdir();
				if( !success ){
					log4j.error("Cannot create destination directory: " + fork );
					log4j.error("Exiting now." );

					return false;
				}

				writeQsubFile( new StringBuffer( outputdir ).append( "/FORK" ).append( i ).toString(), 
						new StringBuffer( outputdir ).append("/blajob").append( i ).append(( retries ? ".retries" : "" )).toString(), 
						ncbiDatabase, minscore, cores, 
						groupId, cutoffs, resolveCultured, slippageTolerance, wordsize, tophits, retries );
			}
	
			if( writer != null ){
				writer.flush();
				writer.close();
			}

			return true;

		} catch (IOException ioEx ){
			ioEx.printStackTrace(System.err);
			log4j.error(ioEx.getMessage());
		}

		return false;
	}

	private static boolean writeQsubFile( String outputdir, String filename, String ncbiDatabase, boolean minscore, int cores, 
			String groupId, String cutoffs, boolean resolveCultured, double slippageTolerance, int wordsize, int tophits, boolean retries ){
// maybe set this up to actually sed the log4j file first?

		StringBuffer qsubCmds = new StringBuffer();
		qsubCmds.append("#!/bin/sh\n");
		qsubCmds.append("#$ -cwd\n");
		qsubCmds.append("#$ -S /bin/bash\n");
		qsubCmds.append(
				"if [ -f $HOME/.profile ]; then\n" ).append(
					"  . $HOME/.profile\n").append(
				"fi\n");
		qsubCmds.append(
				"if [ -f $HOME/.bash_profile ]; then\n" ).append(
					"  . $HOME/.bash_profile\n").append(
				"fi\n");

		qsubCmds.append("java -Xmx1028m -Xms256m -cp `cat cp.txt` marta.taxa.AssignTaxa ")
				.append( filename ).append(
						" -out=" ).append( outputdir ).append(
						( minscore ? " -minscore" : "" )).append( 
						" -co=").append( String.valueOf(cores)).append(
						" -group=" ).append( groupId ).append(
						( cutoffs == null ? "" : " -cutoffs=".concat( cutoffs ))).append(
						( resolveCultured ? "" : " -noresolve" )).append(
						" -forked" ).append(
						" -db=" ).append( ncbiDatabase ).append(
						" -tile=" ).append( slippageTolerance ).append(
						" -ws=" ).append( wordsize ).append(
						" -top=" ).append( tophits ).append(
						" -host=" ).append( "$HOSTNAME" ).toString();

		String qsubFile = new StringBuffer(
				( retries ? "retries" : "" )).append( filename ).append( ".sh" ).toString();

		try{

	        BufferedWriter writer = new BufferedWriter( new OutputStreamWriter( new FileOutputStream( qsubFile, false), "UTF-8"));

	        writer.write(qsubCmds.toString());
	        writer.write('\n');
	        writer.flush();
	        writer.close();

		} catch( IOException ioEx ){
			log4j.error("IOException during java blasting for file: " + filename );
			log4j.error(ioEx.getMessage());
			ioEx.printStackTrace(System.err);
			System.exit(1);
			return false;
		}

		return true;
	}

	// drmaa
	private static boolean putBlastJobsOnQueue( String sourcedir, boolean memarg, boolean retries ){

		SimpleDateFormat fmt = new SimpleDateFormat();
		fmt.setCalendar(Calendar.getInstance());
		fmt.getCalendar().add( Calendar.SECOND, 30 );
		fmt.applyPattern("MMddHHmm");
		String starttime = fmt.format( fmt.getCalendar().getTime());

		class QueueFilter implements FilenameFilter{
			boolean retrying = false;

			public QueueFilter( boolean retries ){
				retrying = retries;
			}

			public boolean accept(File f, String name){
				if( retrying ){
					return( name.startsWith("retries") && name.endsWith(".sh"));

				} else {
					return(name.endsWith(".sh"));
				}
			}
		};
		
		try{

			File dir = new File( sourcedir );
			File[] files = dir.listFiles( new QueueFilter( retries ));

			for( File file : files ){

				String[] cmds = {
						"sh",
						"-c", // how do I delay this... is it `-a yadda`???
						new StringBuffer( "qsub " ).append(" -a ").append( starttime ).append(( memarg ? " -l h_vmem=8g " : " " )).append( file.getAbsolutePath()).toString()
				};
	
				Runtime rt = Runtime.getRuntime();
				Process proc = rt.exec(cmds);
	
				InputStream err = proc.getErrorStream();
		        InputStreamReader reader = new InputStreamReader(err);
		        BufferedReader breader = new BufferedReader(reader);
		        String line; boolean error = false;
		        while(( line = breader.readLine()) != null){
		        	error = true;
		        	log4j.error(line);
		        }
	
		        if( error ){
		        	return false;
		        }
	
		        InputStream out = proc.getInputStream();
		        reader = new InputStreamReader(out);
		        breader = new BufferedReader(reader);
		        while(( line = breader.readLine()) != null){
		        	log4j.info(line);
		        }
	
		        try{
					proc.waitFor();
					log4j.info("TODO: need a check to see if the java program is actually in the queue. proc.waitFor() won't do anything here!: "+ file.getName());
	
				}  catch ( InterruptedException e ) { 
					log4j.error("InterruptedException in launchBlastJobs()!");
					e.printStackTrace(System.err);
					return false;
				}
			}

			return true;

		} catch( IOException ioEx ){
			ioEx.printStackTrace(System.err);
		}

		return false;
	}

	private static String getTime(){
		SimpleDateFormat fmt = new SimpleDateFormat();
		fmt.setCalendar(Calendar.getInstance());
		fmt.applyPattern("MMddyy-HHmmss");
		return fmt.format( fmt.getCalendar().getTime());
	}
}
