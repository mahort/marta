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

public class TaxonSelection {

	private static Logger logger = Logger.getLogger("marta.taxa");
	private static boolean spuhsVote = false;
	static {

	    java.util.Properties props = new java.util.Properties();
	    try {
	    	props.load( new java.io.FileInputStream("marta.properties"));
	    	spuhsVote = Boolean.parseBoolean( props.getProperty("spuhsvote"));

	    } catch( IOException e ){
	       e.printStackTrace();
	       logger.error("Error loading db properties. Make sure that marta.properties is in the classpath and that the properties are correctly labeled.");
	    }
	}

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
					new StringBuffer( getOutputDirectory()).append( "besthits.txt" ).toString(), false ), "UTF-8" ));

			for( String gi : GIs ){
				writer.write( gi );
				writer.write( '\t');
				writer.write( String.valueOf( this.getDescription( gi ).getBitscore())); // include the bitscore too: for long sequences E-values do not separate better alignments (bitscores do).
				writer.write( '\t');
				writer.write( String.valueOf( this.getDescription( gi ).getMinimumEvalue()));
				writer.write( '\n');
			}

			writer.flush();
			writer.close();

//
// sort them...
     		String rFile = new StringBuffer( getOutputDirectory()).append( "/" ).append( getShortId()).append(".R").toString();
	        writer = new BufferedWriter(
	        		new OutputStreamWriter( new FileOutputStream( rFile, false), "UTF-8"));

//
// now write the besthits file per the muscle alignment...
     		StringBuffer rCmds = new StringBuffer();
     		rCmds.append(
     				"data <- read.table(file=\"" ).append( getOutputDirectory()).append( "besthits.txt" ).append( "\"" ).append(
					",fill=T,sep='\t');\n");

// w/ bitscores now, so from largest to smallest!
     		rCmds.append( "data <- data[order(data[,2],decreasing=T),];" ).toString(); // now sorting on score rather than e-value.
     		rCmds.append("write.table(data,\"" ).append(
     					getOutputDirectory()).append("besthits.txt\",sep='\t',quote=F,row.names=F,col.names=F);\n"); // print the output to the console...

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

			String filename = new StringBuffer( getOutputDirectory()).append("besthits.txt").toString();
			BufferedReader besthits = new BufferedReader( new InputStreamReader( new FileInputStream( filename ), "UTF-8"));

//
// write out the voting table...
			writer = new BufferedWriter(
							new OutputStreamWriter( new FileOutputStream( 
							new StringBuffer( getOutputDirectory()).append( "votes." ).append(
									( this.getGroupId() == null ? "nogroup" : getGroupId().trim())).toString(), false), "UTF-8"));

			int dimOne = dim(filename, '\t')[0];
			int i = 0;
			String[][] hits = new String[dimOne][3]; // gi<tab>bitscore<tab>evalue

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

			hits = new String[hitsWithTaxonomy.size()][3];
			hitsWithTaxonomy.toArray( hits );

///////////////////////////////////////////////////////////////////////////////////////////////
// tabulate the votes for valid reads...
///////////////////////////////////////////////////////////////////////////////////////////////
			double score = 0, eValue = 0;
			double topScore = Double.valueOf( hits[0][1] );
			double slipScore = topScore * ( getSlippageTolerance() / 100 );

			TaxaLogger.logDebug( logger, "Using slipscore: " + slipScore, TaxaLogger.getTime(), this.getShortId());
			// when I voted by min-evalue the argument was::: double slipScore = topScore / getSlippageTolerance();

			Vector<String> culturables = new Vector<String>();
			Vector<String> unculturables = new Vector<String>();
			Vector<String[]> names = new Vector<String[]>();

			writer.write("Reviewing sequence-id: " + this.getShortId());
			writer.write('\n');

			boolean amongTies = false;
			for( int j = 0; j < hits.length; j++ ){

///////////////////////////////////////////////////////////////////////////////////////////////
// get the gi's lineage info and vote for each category...
///////////////////////////////////////////////////////////////////////////////////////////////
				String gi = hits[j][0];
				score = Double.valueOf( hits[j][1] );
				eValue = Double.valueOf( hits[j][2] );

				if( j < (hits.length - 1 )){
					amongTies = ( Double.compare( score, Double.valueOf( hits[j+1][1] )) == 0 );

				} else { 
					amongTies = false; // this is irrelevant, but clearer when written.
				}

// not-uncommon for a null pointer @ this line when there's a disconnect b/w the database and the local blast database.
// seems to happen even if the database is BRAND SPANKING NEW FROM THE WEBSITE b/c the taxonomy info is only updated 1x a week
// it is also possible that the GIs are available in the blast-utility supporting files, but not the taxonomy database, because
// of errors during the curation of the databases at ncbi.
				Taxonomy tmp = bestHits.get( gi );
				writer.write( tmp.printLineageAndStatus( gi, String.valueOf( score )));
				writer.write('\n');

///////////////////////////////////////////////////////////////////////////////////////////////
// skip uncultured/unidentified critters. One clue: no genus level assignment!
// CORRECTION: THERE ARE SOME SPP (HITHERTO UNCERTAIN) THAT DO NOT HAVE GENUS LEVEL ASSIGNMENTS, BUT
// 				WHICH SEEM TO BE ACCURATE, GIVEN THE CORPUS TEST.
// QUESTION/TO TEST-CASE: ARE THERE SAMPLES W/ MISSING GENERA AND SPP NODES?
//				THE TAXONOMY DATABASE CONTINUES TO BE CURATED.
///////////////////////////////////////////////////////////////////////////////////////////////
				if( null == tmp.getGenus() ||
					null == tmp.getSpecies() || 
				//if(( null != tmp.getSpecies()) && 
						tmp.getSpecies().toLowerCase().startsWith("Uncultur".toLowerCase()) ||
								tmp.getSpecies().toLowerCase().startsWith("Unknown".toLowerCase()) ||
								tmp.getSpecies().toLowerCase().startsWith("Unidentified".toLowerCase())){

					unculturables.add( gi );

				//} else if( null == tmp.getGenus() && null == tmp.getSpecies()){
					//	unculturables.add( gi );
					//  TaxaLogger.logInfo( logger, "gi: " + gi + " is a hit with missing genus and spp level assignment.", null, this.getShortId());

				} else {
// we only return "name_class='scientific name'" which should be 'having n = 1' for each TaxonId per my 'group by'
// we vote... by TAXONID.
					culturables.add( gi );
					names.add( tmp.getTaxonIds());
				}

				if( !amongTies && culturables.size() == 0 ){
///////////////////////////////////////////////////////////////////////////////////////////////
// HERE: WE DO NOT HAVE VOTES; continue until we hit the slipScore basement.
///////////////////////////////////////////////////////////////////////////////////////////////
					if(( j < hits.length - 1 ) && ( Double.compare( slipScore, Double.valueOf( hits[j+1][1] )) <= 0 )){
						continue;

					} else {
						writer.write("Considered top: " + names.size() + " elements, but we slipped past the slip-score.\nExiting now.\n" );
						break;
					}

				} else if( !amongTies && culturables.size() > 0 ){
///////////////////////////////////////////////////////////////////////////////////////////////
// we have votes, so no need to collect more candidates (since the next score isn't a tie).
///////////////////////////////////////////////////////////////////////////////////////////////
					writer.write("Considered top: " + names.size() + " elements.\n" );
					break; // otherwise.

				} else {
					continue; // keep collecting candidate taxa, since we're among ties...
				}
			}

			String[][] levels = new String[names.size()][8];
			names.toArray(levels);

			if( levels.length == 0 ){
//
// omitted everything under consideration b/c culturables.size() == 0 at the 
// scored-scale of interest (that is to say: if NOT USING a minscore, then, iterating
// over all of the results yielded no cultured critters).
		        this.announceWinner( null, null, null, null,
		        		String.valueOf( eValue ), String.valueOf( score ), String.valueOf( topScore ),
		        		null, null, null, 0, null ); // nb: this works whether or not we aligned.

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
// VOTING-ALORITHM (votes b/w ties) at the spp, genus, family levels AND UP...
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

				int countsAtLevel = 0;
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
// if the currently analyzed read didn't vote at this level (TODO: maybe the call is a SPUH) then ignore it (at this level of voting).
					if( "0".equals( currentVote )){
						if( spuhsVote ){
							countsAtLevel++;
						}

						continue;
					}

					countsAtLevel++;
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

				String taxonid = null;

				Enumeration<String> e = votes.keys(); 
				Integer tally = null;

				while( e.hasMoreElements()){
					taxonid = e.nextElement(); // retrieve the taxon-vote and the number of votes in its favor. 
					tally = votes.get( taxonid );

					double cutoff = (double)getThresholds()[specificity] * countsAtLevel;

					if( Double.compare((double)tally.intValue(), cutoff ) >= 0 ){ // does the vote count exceed our threshold (above)?


						writer.write("Spuhs voting: " + spuhsVote + '\n');
						writer.write("Considering only: " + countsAtLevel + " out of: "+ culturables.size() + " total votes.*\n");

						writer.write("Cutoff: " + getThresholds()[specificity]);
						writer.write('\n');

						writer.write("Count: " + tally.intValue());
						writer.write('\t');
						writer.write("Threshold: "+ String.valueOf( cutoff ));
						writer.write('\n');
						writer.write("*If the taxonomy database doesn't include spuh level info and the user specifies !spuhsVote (marta.properties), then the taxon doesn't vote during that round (level) of voting");
						writer.write('\n');
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
						        		String.valueOf( eValue ),
						        		String.valueOf( score ),
						        		String.valueOf( topScore ), 
						        		this.getDescription( gi ).getPercentageIdentity(),
						        		format(((double) this.getDescription( gi ).getLength()) / this.getTarget().length(), true),
						        		bestHit.getFullTaxonomy( rank ), tally.intValue(), new StringBuffer( String.valueOf( countsAtLevel )).append(":").append(culturables.size()).toString());
						        return true;
							}

							break;

						case GENUS:
							if( bestHit.getGenusTaxonId() == taxonid ){
						        this.announceWinner( gi, taxonid, rank.toString(), bestHit.getGenus(),
						        		String.valueOf( eValue ),
						        		String.valueOf( score ), 
						        		String.valueOf( topScore ), 
						        		this.getDescription( gi ).getPercentageIdentity(),
						        		format(((double) this.getDescription( gi ).getLength()) / this.getTarget().length(), true),
						        		bestHit.getFullTaxonomy( rank ), tally.intValue(), new StringBuffer( String.valueOf( countsAtLevel )).append(":").append(culturables.size()).toString());
								        

						        return true;
							}

							break;

						case FAMILY:
							if( bestHit.getFamilyTaxonid() == taxonid ){
						        this.announceWinner( gi, taxonid, rank.toString(), bestHit.getFamily(),
						        		String.valueOf( eValue ),
						        		String.valueOf( score ),
						        		String.valueOf( topScore ), 
						        		this.getDescription( gi ).getPercentageIdentity(),
						        		format(((double) this.getDescription( gi ).getLength()) / this.getTarget().length(), true),
						        		bestHit.getFullTaxonomy( rank ), tally.intValue(), new StringBuffer( String.valueOf( countsAtLevel )).append(":").append(culturables.size()).toString());
								        

						        return true;
							}

							break;

						case ORDER:
							if( bestHit.getOrderTaxonId() == taxonid ){
						        this.announceWinner( gi, taxonid, rank.toString(), bestHit.getOrder(),
						        		String.valueOf( eValue ),
						        		String.valueOf( score ),
						        		String.valueOf( topScore ), 
						        		this.getDescription( gi ).getPercentageIdentity(),
						        		format(((double) this.getDescription( gi ).getLength()) / this.getTarget().length(), true),
						        		bestHit.getFullTaxonomy( rank ), tally.intValue(), new StringBuffer( String.valueOf( countsAtLevel )).append(":").append(culturables.size()).toString());

								return true;
							}

							break;

						case CLASS:
							if( bestHit.getTaxonclassId() == taxonid ){
						        this.announceWinner( gi, taxonid, rank.toString(), bestHit.getTaxonClass(),
						        		String.valueOf( eValue ),
						        		String.valueOf( score ),
						        		String.valueOf( topScore ), 
						        		this.getDescription( gi ).getPercentageIdentity(),
						        		format(((double) this.getDescription( gi ).getLength()) / this.getTarget().length(), true),
						        		bestHit.getFullTaxonomy( rank ), tally.intValue(), new StringBuffer( String.valueOf( countsAtLevel )).append(":").append(culturables.size()).toString());

								return true;
							}

							break;

						case PHYLUM:
							if( bestHit.getPhylumTaxonId() == taxonid ){
						        this.announceWinner( gi, taxonid, rank.toString(), bestHit.getPhylum(),
						        		String.valueOf( eValue ),
						        		String.valueOf( score ), 
						        		String.valueOf( topScore ), 
						        		this.getDescription( gi ).getPercentageIdentity(),
						        		format(((double) this.getDescription( gi ).getLength()) / this.getTarget().length(), true),
						        		bestHit.getFullTaxonomy( rank ), tally.intValue(), new StringBuffer( String.valueOf( countsAtLevel )).append(":").append(culturables.size()).toString());

								return true;
							}

							break;

						case KINGDOM:
							if( bestHit.getKingdomTaxonid() == taxonid ){
						        this.announceWinner( gi, taxonid, rank.toString(), bestHit.getKingdom(),
						        		String.valueOf( eValue ),
						        		String.valueOf( score ),
						        		String.valueOf( topScore ), 
						        		this.getDescription( gi ).getPercentageIdentity(),
						        		format(((double) this.getDescription( gi ).getLength()) / this.getTarget().length(), true),
						        		bestHit.getFullTaxonomy( rank ), tally.intValue(), new StringBuffer( String.valueOf( countsAtLevel )).append(":").append(culturables.size()).toString());

								return true;
							}

							break;

						case SUPERKINGDOM:
							if( bestHit.getSuperkingdomTaxonid() == taxonid ){
								this.announceWinner( gi, taxonid, rank.toString(), bestHit.getSuperkingdom(),
						        		String.valueOf( eValue ),
						        		String.valueOf( score ),
						        		String.valueOf( topScore ), 
						        		this.getDescription( gi ).getPercentageIdentity(),
						        		format(((double) this.getDescription( gi ).getLength()) / this.getTarget().length(), true),
						        		bestHit.getFullTaxonomy( rank ), tally.intValue(), new StringBuffer( String.valueOf( countsAtLevel )).append(":").append(culturables.size()).toString());

								return true;
							}

							break;
						}
					}
				}
			}

	        this.announceWinner( null, null, null, null, 
	        				String.valueOf( eValue ), String.valueOf( score ), String.valueOf( topScore ), 
	        				null, null, null, 0, null );

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
	private String format( double percent, boolean truncateTo1 ){
		if( Double.compare((double)1, percent) < 0 ){
			return "1.0";

		} else {
			return( df.format( percent ));
		}
	}

	private boolean announceWinner( String gi, String taxonid, String level, String winningTaxon, 
					String evalue, String winningScore, String topScore, String percentIdentity, 
					String coverage, String fullTaxonomy, int votes, String votesOutOf ){

		BufferedWriter writer = null;

		try{

//
// write out the name of the 'winner'.
	        writer = new BufferedWriter(
	        		new OutputStreamWriter( new FileOutputStream( 
	        				new StringBuffer( getOutputDirectory()).append( getShortId()).append( "_winner." ).append((
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
	        writer.write(( evalue != null ? evalue.trim() : "NA" ));
	        writer.write('\t');
	        writer.write(( winningScore != null ? winningScore.trim() : "NA" ));
	        writer.write('\t');
	        writer.write(( topScore != null ? topScore.trim() : "NA" ));
	        writer.write('\t');
	        writer.write(( percentIdentity != null ? percentIdentity.trim() : "NA" ));
	        writer.write('\t');
	        writer.write(( coverage != null ? coverage : "NA" ));
	        writer.write('\t');
	        writer.write(( fullTaxonomy != null ? fullTaxonomy.trim() : "NA" ));
	        writer.write('\t');
	        writer.write(( votes == 0 ? "NA" : String.valueOf( votes )));
	        writer.write('\t');
	        writer.write(( votesOutOf == null ? "NA" : String.valueOf( votesOutOf )));
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
									.append( getOutputDirectory()).append( getShortId()).append(".fas").toString(), false), "UTF-8"));

// writeLengths = new BufferedWriter(
// new OutputStreamWriter(
// new FileOutputStream(
// new StringBuffer().append( getOutputDirectory()).append( getShortId()).append(".lens").toString(), false), "UTF-8"));
//writeLengths.write("id\tlength\tcoverage\n");

			String sequence; StringBuffer buffer;

//
// next write all of the sequences that we'll align against...
			Enumeration<String> e = targets.elements();
			while( e.hasMoreElements()){
				String gi = e.nextElement();

				if( "Query".equals( gi )){
					sequence = getTarget();

				} else {
					sequence = null;
					System.err.println("we don't have the sequences anymore w/ the new megablast format.");
				}

				buffer = new StringBuffer().append(">").append( gi ).append('\n');

				for( int j = 0; j < sequence.length(); j = j + 60 ){ // check the endpoint.
					buffer.append(sequence.substring(j,(j+60 > sequence.length() ? j+(sequence.length()-j): j+60))).append('\n');
				}

				writer.write(buffer.toString());

//writeLengths.write( gi );
//writeLengths.write('\t');
//writeLengths.write( String.valueOf( sequence.length()));
//writeLengths.write('\t');
//writeLengths.write( String.valueOf(((double)sequence.length() / getTarget().length())));
//writeLengths.write('\n');
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
				Thread.sleep(30000); // wait and retry if it didn't work the first time.

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
			break; // review!

		case -1:
			TaxaLogger.logInfo( logger, "No significant hits!", this.getTime(), this.getShortId());
			return true;

		case -2:
			TaxaLogger.logError( logger, "Either (a) unable to make the master blast file or (b) Strange alignment format. Quitting the job!", this.getTime(), this.getShortId());
			return false;
		}

		Vector<String> ids = getTopScores();
		switch(ids.size()){
		case 0:
//
// the winner is 'Filtered', since nothing passed our filter (80% coverage & 97% sequence identity).
			TaxaLogger.logInfo( logger, "Nothing passed the filter.", this.getTime(), getShortId());
			this.announceWinner( null, null, null, "Filtered", null, null, null, null, null, null, 0, null );
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

			if( parseMegablastOutput() != 1 ){
				return false;
			}

			Vector<String> GIs = this.getTopScores();
			TaxaLogger.logInfo(logger, "Revoting with: " + GIs.size() + " reads.", this.getTime(), this.getShortId());

			if( GIs.size() != 0 ){
				this.vote( GIs );

			} else {
				this.announceWinner( null, null, null, "Filtered", null, null, null, null, null, null, 0, null );
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
							new StringBuffer( this.getOutputDirectory()).append( getShortId()).append(".fas").toString()), "UTF-8" ));

			reader.readLine(); // skip header.

			String line;
			StringBuffer seq = new StringBuffer();
	        while(( line = reader.readLine()) != null){
	        	seq.append(line);
	        }

	        line = seq.toString().replaceAll("\n", "");
			this.setTarget( line );

			reader.close();
			reader = null;
			return true;

		} catch(IOException ioEx){
			ioEx.printStackTrace(System.err);
			TaxaLogger.logError(logger, "(io) " + ioEx.getMessage(), this.getTime(), this.getShortId());
		}

		return false;
	}

	private int blast(){

		try{

			boolean success = false;

			File f = new File( getOutputDirectory());
			if( !f.exists()){
//conv:: success = (new File( getOutputDirectory())).mkdir();

				success = (new File( getOutputDirectory())).mkdir();

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

			String[] cmds = {
					"sh",
					"-c",
					new StringBuffer("megablast ").append( this.getBlastCommandLine()).append(
							" -i ").append( getOutputDirectory()).append( getShortId()).append(".fas").append(
							" -o ").append( getOutputDirectory()).append( getShortId()).append(".out").toString()
			};

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

			return parseMegablastOutput();

		} catch( IOException ioEx ){
			ioEx.printStackTrace(System.err);
			TaxaLogger.logError(logger, "(io) " + ioEx.getMessage(), this.getTime(), this.getShortId());
		}

		return -1;
	}


	private int parseMegablastOutput() throws IOException {

		String line;

		BufferedReader reader = null;

		try{

			boolean something = false;

			reader = new BufferedReader( new InputStreamReader( 
					new FileInputStream( 
							new StringBuffer( this.getOutputDirectory()).append( getShortId()).append(".out").toString()), "UTF-8" ));

			while(( line = reader.readLine()) != null ){
				something = true;
				this.addDescription( line);
			}

			if(!something){
				this.announceWinner( null, null, null, "No significant hits", null, null, null, null, null, null, 0, null );
				TaxaLogger.logError( logger, "Nothing in output file from blast.", this.getTime(), getShortId());
				return -1;
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
		}
	}

	private int getHitLength( String gi ){
		return getDescription( gi ).getLength();
	}

	private double getHitScore( String gi ){

		if( !this.descriptions.containsKey( gi )){
			TaxaLogger.logError( logger, "GI: " + gi + " does not have a description in the list.", this.getTime(), getShortId());
			System.out.println("GI: " + gi + " not in the hashtable?!");
			return 1;
		}

		return getDescription(gi).getBitscore();
	}

	private Vector<String> getTopScores(){

		Vector<String> topHits_80pct = new Vector<String>();

// first, make sure that the coverage >= 80%.
		int lengthOfTarget = getTarget().length();
		int necessaryCoverage = (int)(((double)0.80) * lengthOfTarget );

		TaxaLogger.logDebug( logger, "Getting top hits.", this.getTime(), getShortId());
		TaxaLogger.logDebug( logger, "Necessary coverage: " + necessaryCoverage, this.getTime(), getShortId());
		TaxaLogger.logDebug( logger, "Target Length: " + lengthOfTarget, this.getTime(), getShortId());

		Enumeration <String>e = this.descriptions.keys();

		while( e.hasMoreElements()){
			String gi = e.nextElement();
			TaxaLogger.logDebug( logger, "Hit Length: " + gi + " " + getHitLength(gi), this.getTime(), getShortId());

			if( getHitLength( gi ) >= necessaryCoverage ){
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
// new winner! changed from < to > when we changed from minimum E-value to bitscore.
			if( Double.compare( score, winningScore ) > 0 ){
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

	private Hashtable<String, HitDescription> descriptions;
	private HitDescription getDescription( String gi ){
		return this.descriptions.get( gi );
	}

	private void addDescription( String descriptionLine ){

		if( this.descriptions == null ){
			this.descriptions = new Hashtable<String, HitDescription>();
		}

		HitDescription description = new HitDescription( getShortId(), descriptionLine );
		this.descriptions.put( description.getId(), description );
	}

	private String shortId;
	private String getShortId(){
		return shortId;
	}

	public void setShortId( String shortId ){
		this.shortId = shortId;
	}

	private String getOutputDirectory(){
		return new StringBuffer( getSourceDirectory()).append('/').append( "blast_output" ).append('/').toString();
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

	public TaxonSelection(){
		this.fmt = new SimpleDateFormat();
		this.fmt.setCalendar(Calendar.getInstance());
		this.fmt.applyPattern("MMddyy-HHmmss");
	}

	private String getTime(){
		return fmt.format( fmt.getCalendar().getTime());
	}
}