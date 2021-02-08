package marta.taxa;

import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class AssignTaxaJob implements Job {

	private static Logger logger = Logger.getLogger("marta.taxa");

//
// there is a single 'assigntaxajob' for every read. 
// the job is responsible for blasting the read
// and interpreting the result, using the parameters
// specified by the user (minscore or not, etc.)
//int numberOfHits = Integer.parseInt(map.getString("tophits"));
	public void execute( JobExecutionContext cntxt ) throws JobExecutionException {

		String shortid = null;

		try{

			JobDataMap map = cntxt.getJobDetail().getJobDataMap();
			shortid = map.getString("id");

			TaxonSelection vote = new TaxonSelection();
			vote.setShortId( shortid );																// short-id
			vote.setSourceDirectory( map.getString("outputdir"));									// output-directory.
			vote.setGroupId( map.getString("groupid"));
			vote.setTarget( map.getString("sequence"));												// target sequence
			vote.setBlastCommandLine( map.getString("cmdline")); 									// blast command line options.
			vote.setScoreRequirement( Boolean.parseBoolean( map.getString("scorereq")));			// minscore?
			vote.setResolveCultured( Boolean.parseBoolean( map.getString("resolvecultured")));		//TODO:: ALLOW THE USER TO RESTRICT TO TOP-HIT (UNCERTAIN WHEN UNCULTURED);
			vote.setSlippageTolerance( Double.parseDouble( map.getString("percentile")));	// slippage-tolerance to escape out (if minscore=no)
			vote.setThresholds( map.getString("thresholds"));

// the whole shebang... (contrast with revote)
			vote.blastAndVote();
			return;

		} catch( Exception e ){
			TaxaLogger.logError( logger, "Back at main (rare)" + e.getMessage(), TaxaLogger.getTime(), shortid );
			e.printStackTrace(System.err);
			return;
		}
	}

	protected void finalize(){}
}