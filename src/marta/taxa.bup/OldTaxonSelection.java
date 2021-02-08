package marta.taxa;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.text.SimpleDateFormat;

import java.util.Calendar;
import java.util.Enumeration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

public class OldTaxonSelection {

	private static Logger logger = Logger.getLogger("marta.taxa");

	/*
	 * 
	 */
	public boolean vote( Vector<String> GIs ){

		BufferedWriter writer = null;
		String line;

		try{

//
// write out the scores.
			writer = new BufferedWriter( new OutputStreamWriter( new FileOutputStream( 
					new StringBuffer( getFilePrefix()).append( "besthits.txt" ).toString(), false ), "UTF-8" ));

			for( String gi : GIs ){
				writer.write( gi );
				writer.write( '\t');
				writer.write( String.valueOf( this.getDescription( gi ).getScore()));
				writer.write( '\n');
			}

			writer.flush();
			writer.close();

//
// sort them...
     		String rFile = new StringBuffer( getFilePrefix()).append( "/" ).append( getShortId()).append(".R").toString();
	        writer = new BufferedWriter(
	        		new OutputStreamWriter( new FileOutputStream( rFile, false), "UTF-8"));

//
// now write the besthits file per the muscle alignment...
     		StringBuffer rCmds = new StringBuffer();
     		rCmds.append(
     				"data <- read.table(file=\"" ).append( getFilePrefix()).append( "besthits.txt" ).append( "\"" ).append(
					",fill=T,sep='\t');\n");

// from smallest to largest.
     		rCmds.append( "data <- data[order(data[,2]),];" ).toString();
     		rCmds.append("write.table(data,\"" ).append(
     					getFilePrefix()).append("besthits.txt\",sep='\t',quote=F,row.names=F,col.names=F);\n"); // print the output to the console...

	        writer.write(rCmds.toString());
	        writer.flush();
	        writer.close();

	        String[] cmds = new String[]{
					"sh",
					"-c",
					new StringBuffer("R --vanilla < ").append( rFile ).append( " > " ).append( rFile.concat( ".sorted")).toString()
			};

	        Runtime rt = Runtime.getRuntime();
	        Process proc = rt.exec(cmds);

	        line = null;
	        BufferedReader bReader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
	        while(( line = bReader.readLine()) != null){
				TaxaLogger.logError( logger, line, this.getTime(), getShortId());
	        }

	        bReader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
	        while(( line = bReader.readLine()) != null){
				TaxaLogger.logError( logger, line, this.getTime(), getShortId());
	        }

	        try{
				proc.waitFor();

			}  catch ( InterruptedException e ){ 
				e.printStackTrace(System.err);
				TaxaLogger.logError( logger, "interrputed exception (writing sorted scores)", this.getTime(), getShortId());
				return false;
			}

			return(this.vote());

		} catch(IOException ioEx ){
			ioEx.printStackTrace(System.err);
			TaxaLogger.logError( logger, "(io) " + ioEx.getMessage(), this.getTime(), getShortId());
			return false;
		}
	}

	public boolean vote() throws IOException {

		BufferedWriter writer = null;
		String line;

		try{

			String filename = new StringBuffer( getFilePrefix()).append("besthits.txt").toString();
			BufferedReader besthits = new BufferedReader( new InputStreamReader( new FileInputStream( filename ), "UTF-8"));

//
// write out the voting table...
			writer = new BufferedWriter(
							new OutputStreamWriter( new FileOutputStream( 
							new StringBuffer( getFilePrefix()).append( "votes." ).append(
									( this.getGroupId() == null ? "nogroup" : getGroupId().trim())).toString(), false), "UTF-8"));

			int dimOne = dim(filename, '\t')[0];
			int i = 0;
			String[][] hits = new String[dimOne][2];

			while(( line = besthits.readLine()) != null ){
// here are my hits. get the names from the database (below).
				hits[i++] = line.split("\t");
			}

///////////////////////////////////////////////////////////////////////////////////////////////
// get the taxonomic information (lineage, taxaIds) from our ncbi database.
			Hashtable<String,Taxonomy> bestHits = TaxaDatabase.getTaxonomicInformation( 
						this.getShortId(), hits );

///////////////////////////////////////////////////////////////////////////////////////////////
// some GIs will not have taxonomic information in the database. Remove them from the core-array.
			Vector<String[]> hitsWithTaxonomy = new Vector<String[]>();
			for( String[] hit: hits ){
				if( bestHits.containsKey( hit[0] )){
					hitsWithTaxonomy.add( hit );
				}
			}

			hits = new String[hitsWithTaxonomy.size()][2];
			hitsWithTaxonomy.toArray( hits );

///////////////////////////////////////////////////////////////////////////////////////////////
// tabulate the votes for valid reads...
///////////////////////////////////////////////////////////////////////////////////////////////
			double score = 0; 
			double topScore = Double.valueOf( hits[0][1] );
			double slipScore = topScore / getSlippageTolerance();

			Vector<String> culturables = new Vector<String>();
			Vector<String> unculturables = new Vector<String>();
			Vector<String[]> names = new Vector<String[]>();
			for( int j = 0; j < hits.length; j++ ){

///////////////////////////////////////////////////////////////////////////////////////////////
// get the gi's lineage info and vote for each category...
///////////////////////////////////////////////////////////////////////////////////////////////
				String gi = hits[j][0];
				score = Double.valueOf( hits[j][1] );

// not-uncommon for a null pointer @ this line when there's a disconnect b/w the database and the local blast database.
// seems to happen even if the database is BRAND SPANKING NEW FROM THE WEBSITE b/c the taxonomy info is only updated 1x a week
// it is also possible that the GIs are available in the blastall supporting files, but not the taxonomy database, because
// of errors during the curation of the databases at ncbi!
				Taxonomy tmp = bestHits.get( gi );
				writer.write( tmp.printLineageAndStatus( gi, String.valueOf( score )));
				writer.write('\n');

///////////////////////////////////////////////////////////////////////////////////////////////
// skip uncultured/unidentified critters. Clue: no genus level assignment!
///////////////////////////////////////////////////////////////////////////////////////////////
				if( null == tmp.getGenus() ||
					null == tmp.getSpecies() || 
					tmp.getSpecies().toLowerCase().startsWith("Uncultur".toLowerCase()) ||
					tmp.getSpecies().toLowerCase().startsWith("Unknown".toLowerCase()) ||
					tmp.getSpecies().toLowerCase().startsWith("Unidentified".toLowerCase())){

					unculturables.add( gi );

					if(( culturables.size() > 0 ) && ( j < (hits.length - 1))
							&& ( Double.compare( score, Double.parseDouble( hits[j+1][1])) != 0 )){

///////////////////////////////////////////////////////////////////////////////////////////////
// only continue if there are ties...
///////////////////////////////////////////////////////////////////////////////////////////////
						writer.write("Considered top: " + names.size() + " elements.\n" );
						break; // otherwise.

///////////////////////////////////////////////////////////////////////////////////////////////
// could have placed this actually in the for-loop w/ a short-circuit operator, but for verbosity reasons here!!!
					} else if(( culturables.size() == 0 ) && ( j < (hits.length - 1))
							&& ( Double.compare( score, slipScore ) > 0 )){

						writer.write("Considered top: " + names.size() + " elements, but we slipped past the slip-score.\nExiting now." );
						break;
					}

					continue;
				}

				culturables.add( gi );	

// we only return "name_class='scientific name'" which should be 'having n = 1' for each TaxonId per my 'group by'
// therefore, vote either by taxonId or by name. It doesn't matter.
				names.add( tmp.getTaxonIds());

// succeed early if you can (whether or not getScoreRequirement() is true; if we have votes, tally them).
// nb: OF COURSE CULTURABLES.SIZE > 0 HERE, so don't worry about the slip score.
				if(( j < (hits.length - 1)) && //( score != Double.parseDouble( hits[j+1][1]))){
						( Double.compare( score, Double.parseDouble( hits[j+1][1])) != 0 )){ 
// we have votes at this scoring level. only continue if there are ties (otherwise 'succeed' early).
					writer.write("Considered top: " + names.size() + " elements.\n" );
					break; // otherwise.
				}
			}

			String[][] levels = new String[names.size()][8];
			names.toArray(levels);

			if( levels.length == 0 ){
//
// omitted everything under consideration b/c culturables.size() == 0 at the scored-scale of interest (if !minscore, iterating over all results yields no cultured critters).
		        this.announceWinner( null, null, null, null, String.valueOf( score ), null, 0, 0); // nb: this works whether or not we aligned.
		        return true;
			}

//
// ~quick~ transpose;
			String[][] tmp = new String[8][levels.length];
			for( int j = 0; j < levels.length; j++ ){
				for( int k = 0; k < levels[j].length; k++ ){
					tmp[k][j] = levels[j][k];
				}

			} levels = tmp; // transposed.
			
//for( int j = Rank.SPECIES.ordinal(); j >= Rank.PHYLUM.ordinal() ; j-- ){ // start at species and work Up. Hopefully we will -succeed early-
///////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////
// VOTING-ALORITHM (votes b/w e-value ties) at the spp, genus, family levels AND UP...
///////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////
			int specificity;
			for( Rank rank : Rank.values()){ // outputs in order given; here, we work up from SPP, GENUS, FAM, ..., PHYLUM...
//
// this numeric code allows us to treat our Rank - enum as an int (for array management).
				specificity = rank.getCode();

// the taxonomic assignments for this level (e.g. if specificity is SPECIES, this pulls out all of the votes at the SPECIES level).
				String[] votesAtThisLevel = levels[specificity];
				Hashtable<String,Integer> votes = new Hashtable<String,Integer>();

				for( int vote_i = 0; vote_i < votesAtThisLevel.length; vote_i++ ){ // length == number of voters;

					String currentVote = votesAtThisLevel[vote_i];
					
					if( currentVote == null ){
						System.err.println("EXITING NOW. Null Entry From Taxonomy Database.");
						System.err.println("Using element: " + vote_i );
						System.err.println("i.e. taxonId (numeric): " + levels[specificity][vote_i]);
						System.err.println("key: " + currentVote);
						System.err.println("Rank-level: " + rank.toString());
						System.err.println("Folder: " + "" );
						System.exit(1);
					}

//
// if the currently analyzed read didn't vote at this level (e.g. maybe the call is a SPUH) then ignore it (at this level of voting).
					if( "0".equals( currentVote )){ continue; }

					Integer tally;
					if( votes.containsKey( currentVote )){
//
// because we vote over taxon-ids (and assign literal-string names later), this works whether or not there exists a strain-id name concatenated to the spp name.
						tally = votes.get( currentVote ); // e.g. Olpidium (gen) or Olpidium brassicae (spp) votes.

					} else {
						tally = new Integer(0);
					}

					tally++;
					votes.put( currentVote , tally );
				} // votes tallied. now check below to see if anyone exceeded the cut-off scores for this level of specificity.

				double percent = 0;
				String taxonid = null;

				Enumeration<String> e = votes.keys(); 
				Integer tally = null;

				while( e.hasMoreElements()){
					taxonid = e.nextElement(); // retrieve the taxon-vote and the number of votes in its favor. 
					tally = votes.get( taxonid );

					double cutoff = (double)getThresholds()[specificity] * culturables.size();

					if( Double.compare((double)tally.intValue(), cutoff ) >= 0 ){ // does the vote count exceed our threshold (above)?

						writer.write("Culturables size: " + culturables.size());
						writer.write('\t');
						writer.write("Cutoff: " + getThresholds()[specificity]);
						writer.write('\n');

						writer.write("Count: " + tally.intValue());
						writer.write('\t');
						writer.write("Threshold: "+ String.valueOf( cutoff ));
						writer.write('\n');

						percent = ((double)tally.intValue() / culturables.size());
						break;
					}

					taxonid = null;
				}

//
// We already have the taxonomic information (used it for voting) so now use it to declare the winner @ the winning rank.
				if( taxonid != null ){

					Enumeration<String> f = bestHits.keys(); // list of GIs from our original set.
					while( f.hasMoreElements()){

						String gi = f.nextElement();
						Taxonomy bestHit = bestHits.get( gi ); // stored by GI

						switch( rank ){
						case SPECIES:
							if( bestHit.getSpeciesTaxonId() == taxonid ){
						        this.announceWinner( gi, taxonid, rank.toString(), bestHit.getSpeciesAbbreviated(),
						        		String.valueOf( score ), bestHit.getFullTaxonomy( rank ), tally.intValue(), culturables.size());
						        return true;
							}

							break;

						case GENUS:
							if( bestHit.getGenusTaxonId() == taxonid ){
						        this.announceWinner( gi, taxonid, rank.toString(), bestHit.getGenus(),
						        		String.valueOf( score ), bestHit.getFullTaxonomy( rank ), tally.intValue(), culturables.size());

						        return true;
							}

							break;

						case FAMILY:
							if( bestHit.getFamilyTaxonid() == taxonid ){
						        this.announceWinner( gi, taxonid, rank.toString(), bestHit.getFamily(),
						        		String.valueOf( score ), bestHit.getFullTaxonomy( rank ), tally.intValue(), culturables.size());

						        return true;
							}

							break;

						case ORDER:
							if( bestHit.getOrderTaxonId() == taxonid ){
						        this.announceWinner( gi, taxonid, rank.toString(), bestHit.getOrder(),
						        		 String.valueOf( score ), bestHit.getFullTaxonomy( rank ), tally.intValue(), culturables.size());

								return true;
							}

							break;

						case CLASS:
							if( bestHit.getTaxonclassId() == taxonid ){
						        this.announceWinner( gi, taxonid, rank.toString(), bestHit.getTaxonClass(),
						        		String.valueOf( score ), bestHit.getFullTaxonomy( rank ), tally.intValue(), culturables.size());

								return true;
							}

							break;

						case PHYLUM:
							if( bestHit.getPhylumTaxonId() == taxonid ){
						        this.announceWinner( gi, taxonid, rank.toString(), bestHit.getPhylum(),
						        		String.valueOf( score ), bestHit.getFullTaxonomy( rank ), tally.intValue(), culturables.size());

								return true;
							}

							break;

						case KINGDOM:
							if( bestHit.getKingdomTaxonid() == taxonid ){
						        this.announceWinner( gi, taxonid, rank.toString(), bestHit.getKingdom(),
						        		String.valueOf( score ), bestHit.getFullTaxonomy( rank ), tally.intValue(), culturables.size());

								return true;
							}

							break;

						case SUPERKINGDOM:
							if( bestHit.getSuperkingdomTaxonid() == taxonid ){
						        this.announceWinner( gi, taxonid, rank.toString(), bestHit.getSuperkingdom(),
						        		String.valueOf( score ), bestHit.getFullTaxonomy( rank ), tally.intValue(), culturables.size());

								return true;
							}

							break;

						}
					}
				}
			}

	        this.announceWinner( null, null, null, null, String.valueOf( score ), null, 0, 0 );
			return true; // it might be uncertain, but it isn't an error.

		} catch( IOException ioEx ){
			ioEx.printStackTrace(System.err);
			TaxaLogger.logError( logger, "Error during vote: " + ioEx.getMessage(), this.getTime(), getShortId());

		} finally {
			if( writer != null ){
				writer.flush();
				writer.close();
			}
		}

		return false;
	}

	java.text.DecimalFormat df = new java.text.DecimalFormat("#.##");
	private String format( double percent ){
		return( df.format( percent ));
	}

	private boolean announceWinner( String gi, String taxonid, String level, String winningTaxon, String score, String fullTaxonomy, int votes, int votesOutOf ){

		BufferedWriter writer = null;

		try{

//
// write out the name of the 'winner'.
	        writer = new BufferedWriter(
	        		new OutputStreamWriter( new FileOutputStream( 
	        				new StringBuffer( getFilePrefix()).append( "winner." ).append((
	        						getGroupId() == null ? "nogroup" : getGroupId().trim())).toString(), false), "UTF-8"));

	        writer.write( getShortId());
	        writer.write('\t');
	        writer.write(( gi != null ? gi.trim() : "NA"));
	        writer.write('\t');
	        writer.write(( taxonid != null ? taxonid.trim() : "NA"));
	        writer.write('\t');
	        writer.write(( level != null ? level.trim() : "NA"));
	        writer.write('\t');
	        writer.write(( winningTaxon != null ? winningTaxon.trim() : "Uncertain"));
	        writer.write('\t');
	        writer.write(( score != null ? score.trim() : "NA" ));
	        writer.write('\t');
	        writer.write(( fullTaxonomy != null ? fullTaxonomy.trim() : "NA" ));
	        writer.write('\t');
	        writer.write(( votes == 0 ? "NA" : String.valueOf( votes )));
	        writer.write('\t');
	        writer.write(( votesOutOf == 0 ? "NA" : String.valueOf( votesOutOf )));
	        writer.write('\n');

	        writer.flush();
	        writer.close();
	        return true;

		} catch( IOException ioEx ){
			ioEx.printStackTrace(System.err);
			TaxaLogger.logError( logger, "Error while announcing the winner: " + ioEx.getMessage(), this.getTime(), getShortId());

		} finally {}

		return false;
	}

	private boolean makeFasta( Vector<String> targets ) throws IOException {
// we need a converter for the recently constructed alignment files. 
// it seems to be the case that the files have the following format:
// 
// ALIGNMENTS
// \N
// QUERY ---
// hit_id_1
// ..
// hit_id_n

		BufferedWriter writer = null;
		BufferedWriter writeLengths = null;

		try{

			writer = new BufferedWriter(
					new OutputStreamWriter(
							new FileOutputStream(
									new StringBuffer()
									.append( getFilePrefix()).append( getShortId()).append(".fas").toString(), false), "UTF-8"));

			writeLengths = new BufferedWriter(
					new OutputStreamWriter(
							new FileOutputStream(
									new StringBuffer()
									.append( getFilePrefix()).append( getShortId()).append(".lens").toString(), false), "UTF-8"));
			
			writeLengths.write("id\tlength\tcoverage\n");

			String sequence; StringBuffer buffer;

//
// next write all of the sequences that we'll align against...
			Enumeration<String> e = targets.elements();
			while( e.hasMoreElements()){
				String gi = e.nextElement();

				if( "Query".equals( gi )){
					sequence = getTarget();

				} else {
					sequence = this.getSequences().get(gi);
				}

				buffer = new StringBuffer().append(">").append( gi ).append('\n');
// split the sequence into 80 letter fragments.
				for( int j = 0; j < sequence.length(); j = j + 60 ){ // check the endpoint.
					buffer.append(sequence.substring(j,(j+60 > sequence.length() ? j+(sequence.length()-j): j+60))).append('\n');
				}

				writer.write(buffer.toString());

				writeLengths.write( gi );
				writeLengths.write('\t');
				writeLengths.write( String.valueOf( sequence.length()));
				writeLengths.write('\t');
				writeLengths.write( String.valueOf(((double)sequence.length() / getTarget().length())));
				writeLengths.write('\n');
			}

			return true;

		} catch ( IOException ioEx ){
			ioEx.printStackTrace(System.err);
			TaxaLogger.logError( logger, "(io) making fasta file: " + ioEx.getMessage(), this.getTime(), getShortId());
			return false;

		} finally {

			if( writer != null ){
				writer.flush();
				writer.close();
			}

			if( writeLengths != null ){
				writeLengths.flush();
				writeLengths.close();
			}
		}
	}

	public boolean blastAndVote(){

		int s = 0;
		for( int i = 1; i <= 3; i++ ){

			s = this.blast();
			if( s != 0 ){ break; }

			try{
				Thread.sleep(30000);

			} catch(InterruptedException intEx ){
				intEx.printStackTrace(System.err);
			}
		}

		switch( s ){
		case 0:
			TaxaLogger.logError( logger, "Error while blasting!", this.getTime(), this.getShortId());
			return false;

		case 1:
			TaxaLogger.logDebug( logger, "Blast job completed.", this.getTime(), this.getShortId());
			break;

		case -1:
			TaxaLogger.logInfo( logger, "No significant hits!", this.getTime(), this.getShortId());
			return true;

		case -2:
			TaxaLogger.logError( logger, "Either (a) unable to make the master blast file or (b) Strange alignment format. Quitting the job!", this.getTime(), this.getShortId());
			return false;
		}

		Vector<String> ids = getTopHits();
		switch(ids.size()){
		case 0:
//
// the winner is 'Filtered', since nothing passed our filter (80% coverage & 97% sequence identity).
			TaxaLogger.logInfo( logger, "Nothing passed the filter.", this.getTime(), getShortId());
			this.announceWinner( null, null, null, "Filtered", null, null, 0, 0 );
			return true;

		default:

//
// there are multiple hits, so...
			TaxaLogger.logDebug( logger, "Multiple hits. Proceeding.", this.getTime(), getShortId());
		}
	
//
// ASSIGN TAXONOMIC STATUS USING THE TAXONOMY DATABASE.
// if aligning == true, then we will grab the minimum dist from the alignment and write a 'besthits file'.
// if aligning == false, then we will sort the scores (possibly parsed by getTopHits() if minscore==true) and write the 'besthits' file.
// EITHER WAY: this will use the besthits file to vote.
		boolean success = this.vote( ids );
		if( !success ){
			TaxaLogger.logError( logger, "Unable to assign taxonomic information (error).", this.getTime(), getShortId());
			return false;

		} else {
			TaxaLogger.logInfo( logger, "Taxonomic status assigned. Job is done.", this.getTime(), getShortId());
		}

		return true;
	}

	public boolean revote(){

		try{

			this.resetTarget();

			if( parseBlastOutput() != 1 ){
				return false;
			}

			Vector<String> GIs = this.getTopHits();
			TaxaLogger.logInfo(logger, "Revoting with: " + GIs.size() + " reads.", this.getTime(), this.getShortId());

			if( GIs.size() != 0 ){
				this.vote( GIs );

			} else {
				this.announceWinner( null, null, null, "Filtered", null, null, 0, 0 );
			}

			return true;

		} catch(IOException ioEx ){
			ioEx.printStackTrace(System.err);
			TaxaLogger.logError(logger, "(io) " + ioEx.getMessage(), this.getTime(), this.getShortId());
		}

		return false;
	}

	private boolean resetTarget(){

		BufferedReader reader;

		try{
			reader = new BufferedReader( new InputStreamReader( 
					new FileInputStream( 
							new StringBuffer( this.getFilePrefix()).append( getShortId()).append(".seq").toString()), "UTF-8" ));

			this.setTarget(reader.readLine());

			reader.close();
			reader = null;
			return true;

		} catch(IOException ioEx){
			ioEx.printStackTrace(System.err);
			TaxaLogger.logError(logger, "(io) " + ioEx.getMessage(), this.getTime(), this.getShortId());
		}

		return false;
	}

	private boolean writeSeq(){

		BufferedWriter writer = null;
		try{

			writer = new BufferedWriter(
					new OutputStreamWriter(
							new FileOutputStream(
									new StringBuffer().append( getFilePrefix()).append("/").append( getShortId() ).append(".seq").toString(), false), "UTF-8"));

			writer.write( this.getTarget());
			writer.write('\n');
			writer.flush();
			writer.close();
			return true;

		} catch( IOException ioEx ){

			ioEx.printStackTrace(System.err);
			TaxaLogger.logError(logger, "(io) " + ioEx.getMessage(), this.getTime(), this.getShortId());
		}

		return false;
	}

	private int blast(){

		try{

			boolean success = false;

			File f = new File( getFilePrefix() );
			if( !f.exists()){
				success = (new File( getFilePrefix())).mkdir();
				if( !success ){
					TaxaLogger.logError( logger, "Unable to make destination directory while blasting.", this.getTime(), getShortId());
					return 0;
				}
			}

//
// turn our target sequence into a fasta file and then submit it here...
// otherwise, everything below should work.
			Vector<String> sequence = new Vector<String>();
			sequence.add( "Query" );
			if( !this.makeFasta( sequence )){
				TaxaLogger.logError( logger, "Unable to make the master fasta file during blast.", this.getTime(), getShortId());
				return -2;

			} else {} // proceeding

			if( !this.writeSeq()){
				TaxaLogger.logError( logger, "Unable to make the target sequence file during blast.", this.getTime(), getShortId());
			}

			String[] cmds = {
					"sh",
					"-c",
					new StringBuffer("megablast ").append( this.getBlastCommandLine()).append(
							" -i ").append( getFilePrefix()).append( getShortId()).append(".fas").append(
							" -o ").append( getFilePrefix()).append( getShortId()).append(".out").toString()
			};
/*cmds = new String[]{
			"sh",
			"-c",
			new StringBuffer("blastall ").append( this.getBlastCommandLine()).append(
					" -i ").append( getFilePrefix()).append( getShortId()).append(".fas").append(
					" -o ").append( getFilePrefix()).append( getShortId()).append(".out").toString()
	};

} else {*/

			Runtime rt = Runtime.getRuntime();
			Process proc = rt.exec(cmds);

	        String line;
	        BufferedReader bReader = new BufferedReader( new InputStreamReader( proc.getErrorStream()));
	        while(( line = bReader.readLine()) != null){
				TaxaLogger.logError( logger, line, this.getTime(), getShortId());
	        }

	        bReader = new BufferedReader( new InputStreamReader(proc.getInputStream()));
	        while(( line = bReader.readLine()) != null){
				TaxaLogger.logError( logger, line, this.getTime(), getShortId());
	        }

	        try{
				proc.waitFor();

			}  catch ( InterruptedException e ) { 
				TaxaLogger.logError( logger, "interrupted exception during blast.", this.getTime(), getShortId());
				e.printStackTrace(System.err);
				return 0;
			}

			return parseBlastOutput();

		} catch( IOException ioEx ){
			ioEx.printStackTrace(System.err);
			TaxaLogger.logError(logger, "(io) " + ioEx.getMessage(), this.getTime(), this.getShortId());
		}

		return -1;
	}


	private int parseMegablastOutput() throws IOException {

		String line;

		BufferedReader reader = null;
		BufferedWriter writeDescriptions = null;
		BufferedWriter writeAlignments = null;

		try{

			writeDescriptions = new BufferedWriter(
					new OutputStreamWriter(
							new FileOutputStream(
									new StringBuffer().append( getFilePrefix()).append("/").append( getShortId() ).append(".desc").toString(), false), "UTF-8"));

			boolean something = false, descriptions = false;
			reader = new BufferedReader( new InputStreamReader( 
					new FileInputStream( 
							new StringBuffer( this.getFilePrefix()).append( getShortId()).append(".out").toString()), "UTF-8" ));
	
			while(( line = reader.readLine()) != null ){
				something = true;

				if( line.indexOf( "No hits found") != -1 ){
					this.announceWinner( null, null, null, "No significant hits", null, null, 0, 0 );
					return -1;
				}

				writeDescriptions.write(line);
				writeDescriptions.write('\n');
				this.addDescription(line);
			}

			if(!something){ 
				TaxaLogger.logError( logger, "Nothing in output file from blast.", this.getTime(), getShortId());
				return 0;
			}

			return 1;
	
		} catch( IOException ioEx ){
			ioEx.printStackTrace(System.err);
			TaxaLogger.logError( logger, "(io) blast error: " + ioEx.getMessage(), this.getTime(), getShortId());
			return 0;
	
		} catch( Exception ex ){
			ex.printStackTrace(System.err);
			TaxaLogger.logError( logger, "general error during blast: " + ex.getMessage(), this.getTime(), getShortId());
			return 0;
	
		} finally {
	
			if( reader != null ){
				reader.close();
			}
	
			if( writeDescriptions != null ){
				writeDescriptions.flush();
				writeDescriptions.close();
			}
	
			if( writeAlignments != null ){
				writeAlignments.flush();
				writeAlignments.close();
			}
		}
	}
	
	private int parseBlastOutput() throws IOException {

		String line;

		BufferedReader reader = null;
		BufferedWriter writeDescriptions = null;
		BufferedWriter writeAlignments = null;

		try{

			writeDescriptions = new BufferedWriter(
					new OutputStreamWriter(
							new FileOutputStream(
									new StringBuffer().append( getFilePrefix()).append("/").append( getShortId() ).append(".desc").toString(), false), "UTF-8"));
	
			writeAlignments = new BufferedWriter(
					new OutputStreamWriter(
							new FileOutputStream(
									new StringBuffer().append( getFilePrefix()).append("/").append( getShortId() ).append(".align").toString(), false), "UTF-8"));
	
			boolean something = false, alignments = false, descriptions = false;
			reader = new BufferedReader( new InputStreamReader( 
					new FileInputStream( 
							new StringBuffer( this.getFilePrefix()).append( getShortId()).append(".out").toString()), "UTF-8" ));
	
			while(( line = reader.readLine()) != null ){
				something = true;
	
				if( line.startsWith( "Sequences producing significant alignments" )){
					descriptions = true;
					reader.readLine(); // skip over the next blank... which would otherwise undermine the next line...
	
				} else if( descriptions && "".equals(line)){ // we are switching to alignments now...
					descriptions = false; // turn off the recording for desc.
					alignments = true;
	
				} else if( line.indexOf( "No hits found") != -1 ){
					this.announceWinner( null, null, null, "No significant hits", null, null, 0, 0 );
			        return -1;
	
				} else if ( alignments && line.trim().startsWith( "Database: All")){ // if we're in the midst of recording the alignments and the program description shows up again, exit.
					break;
				}
	
				if(( descriptions ) && ( !"".equals( line )) && ( !line.startsWith("Sequences producing significant alignments"))){ // write to the desc file, which will be queried after the muscle alignment
					writeDescriptions.write(line);
					writeDescriptions.write('\n');
	
					this.addDescription(line);
				}
	
				if(( alignments ) && (!"".equals(line)) && (!line.startsWith("1_0"))){
					writeAlignments.write(line);
					writeAlignments.write('\n');
	
	//
	//will have to handle the header somehow...
					String lineOfAlignment = null;
					String[] fields = line.replaceAll("[\\s]+"," ").split( " " );
					if( fields.length == 4 || fields.length == 3 ){
						lineOfAlignment = fields[2].replaceAll("-", "");
	
					} else if( fields.length == 2 ){
						lineOfAlignment = fields[1].replaceAll("-", "");
	
					} else {
						TaxaLogger.logError( logger, "odd alignment file format during blast. Exiting job.", this.getTime(), getShortId());
						return -2;
					}
	
					this.addSequence( fields[0], lineOfAlignment );
				}
			}

			if(!something){ 
				TaxaLogger.logError( logger, "Nothing in output file from blast.", this.getTime(), getShortId());
				return 0;
			}

			return 1;
	
		} catch( IOException ioEx ){
			ioEx.printStackTrace(System.err);
			TaxaLogger.logError( logger, "(io) blast error: " + ioEx.getMessage(), this.getTime(), getShortId());
			return 0;
	
		} catch( Exception ex ){
			ex.printStackTrace(System.err);
			TaxaLogger.logError( logger, "general error during blast: " + ex.getMessage(), this.getTime(), getShortId());
			return 0;
	
		} finally {
	
			if( reader != null ){
				reader.close();
			}
	
			if( writeDescriptions != null ){
				writeDescriptions.flush();
				writeDescriptions.close();
			}
	
			if( writeAlignments != null ){
				writeAlignments.flush();
				writeAlignments.close();
			}
		}
	}

	private int getHitLength( String gi ){
		return getSequences( gi ).length();
	}

	private double getHitScore( String gi ){

		if( !this.descriptions.containsKey( gi )){
			TaxaLogger.logError( logger, "GI: " + gi + " does not have a description in the list.", this.getTime(), getShortId());
			System.out.println("GI: " + gi + " not in the hashtable?!");
			return 1;
		}

		return getDescription(gi).getScore();
	}

//
// to be considered as a top hit, the read must have coverage > 80%
// otherwise, the winner will be ambiguous and returned as 'Uncertain'.
// if there are no ties, topHits.size() == 1 and we won't worry with 
// a secondary (full) alignment.
	private Vector<String> getTopHits(){

		Vector<String> topHits_80pct = new Vector<String>();

// first, make sure that the coverage >= 80%.
		int lengthOfTarget = getTarget().length();
		int necessaryCoverage = (int)(((double)0.8) * lengthOfTarget );

		TaxaLogger.logDebug( logger, "Getting top hits.", this.getTime(), getShortId());
		TaxaLogger.logDebug( logger, "Coverage: " + necessaryCoverage, this.getTime(), getShortId());
		TaxaLogger.logDebug( logger, "Target Length: " + lengthOfTarget, this.getTime(), getShortId());

		Enumeration <String>e = sequences.keys();
		while( e.hasMoreElements()){
			String gi = e.nextElement();
			TaxaLogger.logDebug( logger, "Hit Length: " + gi + getHitLength(gi), this.getTime(), getShortId());

			if( getHitLength(gi) >= necessaryCoverage ){
				TaxaLogger.logDebug( logger, "adding gi (nec. coverage): " + gi, this.getTime(), getShortId());
				topHits_80pct.add(gi);
			}
		}

		if( !getScoreRequirement()){
			TaxaLogger.logDebug( logger, "Hits under consideration: " + topHits_80pct.size(), this.getTime(), getShortId());
			return topHits_80pct;
		}

//
// next, check to see whether or not we have ties to align, or if there is a clear winner...
		Vector<String> topHits_bestScores = new Vector<String>();

		double winningScore = 1;
		e = topHits_80pct.elements();
		while( e.hasMoreElements()){
			String gi = (String)e.nextElement();

			double score = getHitScore(gi);
			TaxaLogger.logDebug( logger, "Score for gi: " + gi + " " + score, this.getTime(), getShortId());

//
// new winner!
			if( Double.compare( score, winningScore ) < 0 ){
				topHits_bestScores = new Vector<String>();
				winningScore = score;
				topHits_bestScores.add( gi );

			} else if( Double.compare( score, winningScore) == 0 ){
				topHits_bestScores.add( gi );
			}
		}

// are there ties? or is there one unique winner?? let the caller decide...
		return topHits_bestScores;
	}

	private Hashtable<String, String> sequences;
	private Hashtable<String, String> getSequences(){
		return sequences;
	}

	private String getSequences( String gi ){
		return sequences.get(gi);
	}

	private void addSequence( String gi, String sequencefrag ){
		if( this.sequences == null ){
			this.sequences = new Hashtable<String, String>();
		}

		if( this.sequences.containsKey( gi )){
			String seq = this.sequences.get( gi );
			this.sequences.put( gi, seq.concat( sequencefrag ));

		} else {
			this.sequences.put( gi, sequencefrag );
		}
	}

	private Hashtable<String,OldHitDescription> descriptions;
	private OldHitDescription getDescription( String gi ){
		return this.descriptions.get( gi );
	}

	private void addDescription( String descriptionLine ){
		if( this.descriptions == null ){
			this.descriptions = new Hashtable<String, OldHitDescription>();
		}

		OldHitDescription description = new OldHitDescription( this.getShortId(), descriptionLine );
		this.descriptions.put( description.getId(), description );
	}

	private String shortId;
	private String getShortId(){
		return shortId;
	}

	public void setShortId( String shortId ){
		this.shortId = shortId;
	}

	private String getFilePrefix(){
		return new StringBuffer( getSourceDirectory()).append('/').append( getShortId()).append('/').toString();
	}

	private String blastCommandLine;
	private String getBlastCommandLine(){
		return blastCommandLine;
	}

	public void setBlastCommandLine( String blastCommandLine ){
		this.blastCommandLine = blastCommandLine;
	}

	private boolean scoreRequirement;
	private boolean getScoreRequirement(){
		return scoreRequirement;
	}

	public void setScoreRequirement( boolean scoreRequirement ){
		this.scoreRequirement = scoreRequirement;
	}

	private String target;
	private String getTarget(){
		return target;
	}

	public void setTarget( String target ){
		this.target = target;
	}

	private int percentageIdentity = 0;
	private int getPercentageIdentity(){
		return percentageIdentity;
	}

	public void setPercentageIdentity( int percentageIdentity ){
		this.percentageIdentity = percentageIdentity;
	}

	double[] thresholds =  new double[]{ 1, 1, 1, 1, 1, 1, (double)2/3, (double)2/3 };
	private double[] getThresholds(){
		return thresholds;
	}

	public void setThresholds( String cutoffs ){
		String[] cuts = (cutoffs.split(","));

		this.thresholds = new double[8];
		for( int i = 0; i < cuts.length; i++ ){
			if( cuts[i].indexOf("/") != -1 ){
				String[] dividing = cuts[i].split("/");
				double num = Double.parseDouble( dividing[0] );
				double denom = Double.parseDouble( dividing[1] );
				thresholds[i] = (double)num/denom;

			} else {
				thresholds[i] = Double.parseDouble(cuts[i]);
			}
		}
	}

	public void setThresholds( double[] cutoffs ){
		if( cutoffs.length == 6 ){
			this.thresholds = cutoffs;
		}
	}

	private String sourcedir;
	private String getSourceDirectory(){
		return sourcedir;
	}

	public void setSourceDirectory( String sourcedir ){
		this.sourcedir = sourcedir;
	}

	private String groupId;
	private String getGroupId(){
		return groupId;
	}

	public void setGroupId( String groupId ){
		this.groupId = groupId;
	}

	private double slippageTolerance;
	private double getSlippageTolerance(){
		return slippageTolerance;
	}

	public void setSlippageTolerance( double slippageTolerance ){
		this.slippageTolerance = slippageTolerance;
	}

	private boolean resolveCultured;
	private boolean getResolveCultured(){
		return resolveCultured;
	}

	public void setResolveCultured( boolean resolveCultured ){
		this.resolveCultured = resolveCultured;
	}

	private enum METHOD {
		MINIMUMEVALUE, BITSCORE;
	}

	private METHOD method;
	private METHOD getMethod(){
		return method;
	}

	public void setMethod( METHOD method ){
		this.method = method;
	}

	private int[] dim( String filename, char delim ) throws IOException {

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

	private static SimpleDateFormat fmt;

	public OldTaxonSelection(){
		this.fmt = new SimpleDateFormat();
		this.fmt.setCalendar(Calendar.getInstance());
		this.fmt.applyPattern("MMddyy-HHmmss");
	}

	private String getTime(){
		return fmt.format( fmt.getCalendar().getTime());
	}

// database properties for the jdbc-mysql driver.
	private String jdbcURL;
	private String getJdbcURL(){
		return jdbcURL;
	}

	public void setJdbcURL( String jdbcURL ){
		this.jdbcURL = jdbcURL;
	}

	private String dbUserId;
	private String getDbUserId(){
		return dbUserId;
	}

	public void setDbUserId( String dbUserId ){
		this.dbUserId = dbUserId;
	}

	private String dbPassword;
	private String getDbPassword(){
		return dbPassword;
	}

	public void setDbPassword( String dbPassword ){
		this.dbPassword = dbPassword;
	}

	private String dbDriver;
	private String getDbDriver(){
		return dbDriver;
	}

	public void setDbDriver( String dbDriver ){
		this.dbDriver = dbDriver;
	}
}