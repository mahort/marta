/*
 * by Matt Horton
 * 
 * Revote.class enables the researcher to modify voting parameters (and revote) 
 * using sequences that have already been BLASTed (and avoiding the BLAST bottleneck).
 */
package marta.taxa;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleTrigger;

public class Revote {

	static Logger log4j = Logger.getLogger("marta.taxa");

	public static void main(String[] args){

		boolean oneset = false;
		boolean minscore = false, parallel = false, qsubMemoryRequirement = false;
		boolean recall = false, resolveCultured = true, forked = false;

		double percentile = 98;
		int cores = 3;

		String cutoffs = "1,1,1,1,1,1,2/3,2/3";
		String groupId = null, host = null;
		String sourcedir = "~/"; // require the headdir to be the one containing the S***** FILES. parallel will iterate over the sets & forks.

		if( args.length > 0 ){

			StringBuffer options = new StringBuffer();
			for( int i = 0; i < args.length; i++ ){
				options.append(args[i]).append(" ");

			} options.append(" "); // one terminal space to match on for the last option.

			String arg = options.toString(), tmp;

			forked = ( arg.indexOf("-forked") != -1 );
			minscore = ( arg.indexOf("-minscore") != -1 );
			oneset = ( arg.indexOf("-set") != -1 );
			parallel = ( arg.indexOf("-parallel") != -1 );
			qsubMemoryRequirement = ( arg.indexOf("-qsubmem") != -1);
			recall = ( arg.indexOf("-recall") != -1 );
			resolveCultured = ( arg.indexOf("-noresolve") == -1 );

			if( arg.indexOf("-co=") != -1){
				tmp = arg.substring(arg.indexOf("-co=") + "-co=".length(), arg.indexOf(" ",arg.indexOf("-co=")));
				cores = Integer.parseInt(tmp);
			}

			if( arg.indexOf("-group=") != -1){
				groupId = arg.substring(arg.indexOf("-group=") + "-group=".length(), arg.indexOf(" ",arg.indexOf("-group=")));
			}

			if( arg.indexOf("-tile=") != -1){
				tmp = arg.substring(arg.indexOf("-tile=") + "-tile=".length(), arg.indexOf(" ",arg.indexOf("-tile=")));
				percentile = Double.parseDouble(tmp);
			}

			if( arg.indexOf("-host") != -1 ){
				host = arg.substring(arg.indexOf("-host=") + "-host=".length(), arg.indexOf(" ",arg.indexOf("-host=")));
			}

			if( arg.indexOf("-cutoffs=") != -1 ){
				cutoffs = arg.substring(arg.indexOf("-cutoffs=") + "-cutoffs=".length(), arg.indexOf(" ",arg.indexOf("-cutoffs=")));
			}

			if( arg.indexOf("-src=") != -1){
				sourcedir = arg.substring(arg.indexOf("-src=") + "-src=".length(), arg.indexOf(" ",arg.indexOf("-src=")));

				java.io.File f = new java.io.File(sourcedir);
				if( !f.exists()){
					System.err.println("Output directory: "+ sourcedir + " does not exist");
					System.err.println("-------------------------------------------------");
					System.exit(1);
				}

			} else if( arg.indexOf("--examples") == -1 ){
				System.err.println("Must specify a source directory using argument -src!");
				System.exit(1);
			}

			if( arg.indexOf("--examples") != -1 ){
				System.out.println("example usage (all opts):");
				System.out.println("java -Xmx1028m -Xms256m -cp ~args~ marta.taxa.Revote -src= -tile= -co= -minscore -parallel -qsubmem -recall -noresolve -cutoffs= -group=");
				System.exit(0);
			}

		} else {
			System.err.println("Usage: marta.taxa.Revote srcdir [optional args, e.g. loga, slip-score, # of cores, qsubMemory-req, parallel, etc.]");
			System.exit(1);
		}

		if( forked ){
			FileAppender appender = (FileAppender)log4j.getAppender("myAppender");

			if(( appender != null ) && ( appender.getFile() != null )){
				String logger = new StringBuffer(
						( appender.getFile().indexOf("/") != -1 ? appender.getFile().substring( appender.getFile().lastIndexOf("/") + 1 ) : appender.getFile().trim())).toString();
				logger = sourcedir.concat("_").concat( host ).concat(".revo");

				log4j.info("Redirecting output to: " + logger );

				appender.setFile( logger );
				appender.activateOptions();

			} else {
				log4j.error( "Unable to locate the fileappender 'myAppender'. Please review log4j.properties file.");
			}
		}

		try{

			if( parallel ){
//
// split the file up and recall with qsub.
				boolean success = false;
				if( oneset ){
					success = revoteInParallelForASingleSet( sourcedir, cores, minscore, recall, percentile, cutoffs, resolveCultured, groupId );
					if( !success ){
						System.err.println("Unable to partition the blast file into smaller partitions for cluster-sequencing");
						System.exit(1);
					}

					success = putRevoteJobsOnQueue( sourcedir, qsubMemoryRequirement );
					if( !success ){
						System.err.println("Unable to queue the blast jobs.");
						System.exit(1);
					}

				} else {
					log4j.info("Revoting over all sets!");

					success = revoteInParallel( sourcedir, cores, minscore, recall, percentile, cutoffs, resolveCultured, groupId );
					if( !success ){
						System.err.println("Unable to partition the blast file into smaller partitions for cluster-sequencing");
						System.exit(1);
					}
				}

				return;
			}

			log4j.info("Using source directory: " + sourcedir );
			log4j.info("Revoting with group-id: "+ groupId );
			log4j.info("Revoting with Thresholds: "+ cutoffs );

			/*
			 * 
			 * here we schedule this source-dir for revoting (if parallel, then this is a fork directory that we'll search through).
			 */
			SchedulerFactory schedFact = new org.quartz.impl.StdSchedulerFactory();
			Scheduler sched = schedFact.getScheduler();
			sched.start();

			SimpleTrigger trigger;

			long startTime;
			JobDetail revoteJob = new JobDetail( sourcedir.substring(sourcedir.lastIndexOf("/")), null, RevoteJob.class );

			revoteJob.getJobDataMap().put("sourcedir", sourcedir );							// source directory chosen by user.
			revoteJob.getJobDataMap().put("percentile", String.valueOf(percentile));
			revoteJob.getJobDataMap().put("thresholds", String.valueOf(cutoffs));
			revoteJob.getJobDataMap().put("resolvecultureds", String.valueOf(resolveCultured));
			revoteJob.getJobDataMap().put("minscore", String.valueOf(minscore));
			revoteJob.getJobDataMap().put("recall", String.valueOf(recall));
			revoteJob.getJobDataMap().put("groupid", groupId );

			startTime = System.currentTimeMillis();
			trigger = new SimpleTrigger(new StringBuffer("t").append(sourcedir.substring(sourcedir.lastIndexOf("/"))).toString(),
											null, // I am not providing a 'group' name...
											new java.util.Date(startTime), // this is my start time...
											null, 0, 0L); // no endtime, repeatcount, repeatinterval provided for 1x

			sched.scheduleJob( revoteJob, trigger );

// rather than programming the scheduler to shutdown cleanly by iterating through the jobs associated with the scheduler,
// finding the triggers and checking the status on each... just wait for the user to kill the program cleanly.

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

		} catch( Exception ex ){
			log4j.error( "Error occurred at "+ getTime());
			ex.printStackTrace(System.err);
			log4j.error("Ex message:  " + ex.getMessage());
			System.exit(1);
			
		} finally {}

		return;
	}

	private static boolean revoteInParallelForASingleSet( String sourcedir, int cores, boolean minscore, 
			boolean recall, double slippage, String cutoffs, boolean resolveCultured, String groupId ){
//
//this method is responsible for splitting the files up -and-
//firing the job for queue-submission...
		try{

			//
			// this 'forked' flag is called internally by the qsub scripts (hidden) below. the purpose is to avoid repeated writes to the history
			// file which would otherwise occur (1.) when the user calls w/ -parallel option and (2.) when the qsub script refires this same code.
			StringBuffer options = new StringBuffer();
			options.append(">>>>Revoting<<<<").append('\n');
			options.append( "cores: " ).append( String.valueOf( cores )).append('\n');
			options.append( "source-dir: " ).append( sourcedir ).append('\n');
			options.append( "recall?: " ).append( recall ).append('\n');
			options.append( "minscore: " ).append( minscore ).append('\n');

			if( !minscore ){
				options.append( "\tslip-percentile: " ).append( String.valueOf( slippage )).append('\n');
			}

			options.append( "resolve-cultured: " ).append( String.valueOf( resolveCultured )).append('\n');
			options.append( "Thresholds per rank: " ).append( String.valueOf( cutoffs )).append('\n');

			TaxaLogger.updateVotingHistory( log4j, sourcedir, groupId, options.toString());

			FilenameFilter filterForForks = new FilenameFilter(){
				public boolean accept(File f, String name){
					try{
						return( new File( f.getCanonicalPath().concat("/").concat( name )).isDirectory() && name.startsWith("FORK"));

					} catch(IOException ioEx ){
						ioEx.printStackTrace(System.err);
						return false;
					}
				}
			};

			File dir = new File(sourcedir);
			File[] forks = dir.listFiles(filterForForks);
			for( File fork : forks ){
//hand the forks off to the jobs, which will separately iterate through those files.
				writeQsubFile( sourcedir, fork.getCanonicalPath(), cores, minscore, recall, slippage, cutoffs, resolveCultured, groupId );
			}

			return true;

		} catch( IOException ioEx ){
			ioEx.printStackTrace(System.err);
		}

		return false;
	}


	private static boolean revoteInParallel( String sourcedir, int cores, boolean minscore, 
						boolean recall, double slippage, String cutoffs, boolean resolveCultured, String groupId ){
//
// this method is responsible for splitting the files up -and-
// firing the job for queue-submission...
		try{

			FilenameFilter filterOverSets = new FilenameFilter(){
				public boolean accept(File f, String name){
					return(f.isDirectory() && name.startsWith("set"));
				}
			};
	
			FilenameFilter filterForForks = new FilenameFilter(){
				public boolean accept(File f, String name){
					try{
						return( new File( f.getCanonicalPath().concat("/").concat( name )).isDirectory() && 
										"FORK".equalsIgnoreCase(name.substring(0,4)));

					} catch(IOException ioEx ){
						ioEx.printStackTrace(System.err);
						return false;
					}
				}
			};

			boolean success = false;
			File dir = new File(sourcedir);
			File[] sets = dir.listFiles(filterOverSets);

			if( sets.length == 0 ){ log4j.error("This isn't the source directory! There are no 'sets'! Consider reexecuting with the -oneset option."); }
			for( File set : sets ){
				StringBuffer options = new StringBuffer();
				options.append( ">>>>Revoting<<<<" ).append('\n');
				options.append( "cores: " ).append( String.valueOf( cores )).append('\n');
				options.append( "source-dir: " ).append( sourcedir ).append('\n');
				options.append( "recall?: " ).append( recall ).append('\n');
				options.append( "minscore: " ).append( minscore ).append('\n');

				if( !minscore ){
					options.append( "\tslip-percentile: " ).append( String.valueOf( slippage )).append('\n');
				}

				options.append( "resolve-cultured: " ).append( String.valueOf( resolveCultured )).append('\n');
				options.append( "Thresholds per rank: " ).append( String.valueOf( cutoffs )).append('\n');

				TaxaLogger.updateVotingHistory( log4j, set.getCanonicalPath(), groupId, options.toString());

				File[] forks = set.listFiles(filterForForks);
				for( File fork : forks ){

// hand the forks off to the jobs, which will separately iterate through those files.
					success = writeQsubFile( new StringBuffer( sourcedir ).append('/').append(set.getName()).toString(), fork.getCanonicalPath(), cores, minscore, recall, slippage, cutoffs, resolveCultured, groupId );
					if( success ){
						if( !putRevoteJobsOnQueue( set.getCanonicalPath(), false )){
							log4j.error( "Unable to Q the shell file(s)" );
							return false;
						}

					} else {
						log4j.error( "Unable to write the qsub file(s)." );
						return false;
					}
				}
			}

			return true;

		} catch( IOException ioEx ){
			ioEx.printStackTrace(System.err);
		}

		return false;
	}


	private static boolean writeQsubFile( String sourcedir, String forkdir, int cores, boolean minscore, boolean recall, double slippage, String cutoffs, boolean resolveCultured, String groupId ){
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

		System.out.println("cutoffs: " + cutoffs );

		qsubCmds.append("java -Xmx1028m -Xms256m -cp `cat cp.txt` marta.taxa.Revote ").append(
						" -src=" ).append( forkdir ).append(
						" -tile=" ).append( String.valueOf( slippage )).append(
						( minscore == false ? "" : " -minscore")).append(
						( recall == false ? "" : " -recall")).append(
						( cutoffs == null ? "" : " -cutoffs=")).append( cutoffs ).append(
						( groupId == null ? "" : " -group=".concat( groupId.trim()))).append( 
						" -host=$HOSTNAME" ).append( 
						" -forked" ).append(
						" -co=").append( String.valueOf(cores)).toString();
		
		String qsubFile = new StringBuffer( sourcedir ).append( "/revote" ).append( forkdir.substring(forkdir.lastIndexOf("/") + 1)).append( ".sh" ).toString();
		log4j.info("Writing qsub-file: "+ qsubFile );
		try{

	        BufferedWriter writer = new BufferedWriter( new OutputStreamWriter( new FileOutputStream( qsubFile, false), "UTF-8"));

	        writer.write(qsubCmds.toString());
	        writer.write('\n');
	        writer.flush();
	        writer.close();

		} catch( IOException ioEx ){
			log4j.error(ioEx.getMessage());
			ioEx.printStackTrace(System.err);
			log4j.error("Error during revote for directory: " + forkdir );
			System.exit(1);
			return false;
		}

		return true;
	}

	private static boolean putRevoteJobsOnQueue( String sourcedir, boolean memarg ){

		SimpleDateFormat fmt = new SimpleDateFormat();
		fmt.setCalendar(Calendar.getInstance());
		fmt.getCalendar().add( Calendar.SECOND, 30 );
		fmt.applyPattern("MMddHHmm");
		String starttime = fmt.format( fmt.getCalendar().getTime());

		FilenameFilter filter = new FilenameFilter(){
			public boolean accept(File f, String name){
				return(name.startsWith("revote") && name.endsWith(".sh"));
			}
		};

		try{

			File dir = new File( sourcedir );

			File[] files = dir.listFiles(filter);

			for( File file : files ){

				String[] cmds = {
						"sh",
						"-c", // how do I delay this... is it `-a yadda`???
						new StringBuffer( "qsub " ).append(" -a ").append( starttime ).append(( memarg ? " -l h_vmem=8g " : " " )).append( file.getAbsolutePath()).toString()
				};
	
				Runtime rt = Runtime.getRuntime();
				Process proc = rt.exec(cmds);
				int result = 0;

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
					result = proc.waitFor();
					log4j.info("TODO: need a check to see if the java program is actually in the queue. proc.waitFor() won't do anything here!: "+ file.getName());

				}  catch ( InterruptedException e ) { 
					log4j.error("Result of waiting for queuer: " + result );
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
