package marta.taxa;

import java.io.File;
import java.io.FileFilter;

import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class RevoteJob implements Job {

	private static Logger logger = Logger.getLogger("marta.taxa");

	public void execute( JobExecutionContext cntxt ) throws JobExecutionException {

		String iteration = null;

		try{

			JobDataMap map = cntxt.getJobDetail().getJobDataMap();

			String sourceDirectory = map.getString("sourcedir");
			boolean recall = Boolean.parseBoolean( map.getString("recall"));

			FileFilter filter = new FileFilter(){
				public boolean accept(File f){
					return(f.isDirectory());
				}
			};

			File dir = new File( sourceDirectory );
			File[] files = dir.listFiles(filter); // our list of directories, in the form S1231323/winner.txt
			for( File file : files ){
				TaxonSelection booth = new TaxonSelection();
				iteration = file.getName();

				booth.setShortId( iteration );
				booth.setSourceDirectory( sourceDirectory );

				booth.setGroupId( map.getString("groupid"));
				booth.setResolveCultured( Boolean.parseBoolean( map.getString("resolvecultured")));
				booth.setSlippageTolerance( Double.parseDouble( map.getString("percentile")));
				booth.setThresholds( map.getString("thresholds"));

//
// if we are in 'recall' we reopen the original blast-output file -and-
// parse it, and possibly toggle the minscore option
				if( recall ){
					booth.setScoreRequirement( Boolean.parseBoolean( map.getString("minscore")));
				}

				booth.revote();
				logger.info("Id: " + iteration + " revoted.");
			}

		} catch(Exception ex){
			logger.error( "Error for item: " + iteration );
			logger.error( "Exception in main (rare): " + ex.getMessage());
			ex.printStackTrace(System.err);
		}
	}
}
