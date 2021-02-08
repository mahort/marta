package marta.taxa;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.Logger;

public class Fetch {

	private static Logger logger = Logger.getLogger("marta.taxa");

	public static void main( String[] args ){
// the purpose of this routine is to pull-down the full-length reads for a given GI
// and to make a fasta file for the list of GIs requested (nucleotide only, we won't need protein from efetch).
// first things first, get the requested GIs by parsing whichever file the user submits, take the index 1 (0) as 
// default unless the user provides an alternative.

		int giindex = 1, shortIdIndex = 0, shoId = 0;
		String outputFile = null, sourcefile = null, intermediateFile = null, pattern = null;

		if( args.length > 0 ){
			if( args[0].equals("--examples")){
				System.out.println("example usage (all opts):");
				System.out.println("java -Xmx1028m -Xms256m -cp ~args~ marta.taxa.Fetch file -giindex= -shortidindex= -o=" );
				System.exit(0);
			}

			sourcefile = args[0];

			StringBuffer options = new StringBuffer();
			for( int i = 1; i < args.length; i++ ){
				options.append(args[i]).append(" ");

			} options.append(" "); // one terminal space to match on for the last option.

			String arg = options.toString(), tmp;

			if( arg.indexOf("-giindex=") != -1){
				tmp = arg.substring(arg.indexOf("-giindex=") + "-giindex=".length(), arg.indexOf(" ",arg.indexOf("-giindex=")));
				giindex = Integer.parseInt(tmp);
				giindex--; // expect the user to input a 1-based index.
			}

			if( arg.indexOf("-shortidindex=") != -1){
				tmp = arg.substring(arg.indexOf("-shortidindex=") + "-shortidindex=".length(), arg.indexOf(" ",arg.indexOf("-shortidindex=")));
				shortIdIndex = Integer.parseInt(tmp);
				shortIdIndex--; // expect the user to input a 1-based index.
			}

			if( arg.indexOf("-shoid=") != -1){
				tmp = arg.substring(arg.indexOf("-shoid=") + "-shoid=".length(), arg.indexOf(" ",arg.indexOf("-shoid=")));
				shoId = Integer.parseInt(tmp);
				shoId--; // expect the user to input a 1-based index.
			}

			if( arg.indexOf("-o=") != -1){
				outputFile = arg.substring(arg.indexOf("-o=") + "-o=".length(), arg.indexOf(" ",arg.indexOf("-o=")));

			} else {
				outputFile = sourcefile.concat(".fas");
			}

			if( arg.indexOf("-intermediate=") != -1){
				intermediateFile = arg.substring(arg.indexOf("-intermediate=") + "-intermediate=".length(), arg.indexOf(" ",arg.indexOf("-intermediate=")));
			}

			if( arg.indexOf("-regexpattern=") != -1){
				pattern = arg.substring(arg.indexOf("-regexpattern=") + "-regexpattern=".length(), arg.indexOf(" ",arg.indexOf("-regexpattern=")));
			}

		} else {
			System.err.println("java -Xmx1028m -Xms256m -cp ~args~ marta.taxa.Fetch file -giindex= -shortidindex= -o=" );
			System.exit(1);
		}

		logger.info( "Using source-file: " + sourcefile );

//
// parse the file at the index provided.
		if( intermediateFile == null ){
			logger.info( "Creating new fasta file: " + outputFile );

			String[][] GIs = readGIs( sourcefile, giindex, shortIdIndex );
	
			if( GIs.length > 0 ){
	
				boolean success = getFullLengthSequence( outputFile, GIs );
				if( !success ){
					logger.error("Unable to retrieve the full-length sequences for the requested GIs.");
	
				} else {
					logger.info( "The sequences for your GIs have been downloaded." );
				}
	
			} else {
//
// report file-size error
				logger.error( "Unable to parse the source-file. No GIs found. :(" );
			}

		} else {
			findGIsForFiles( pattern, sourcefile, intermediateFile, shoId, giindex, shortIdIndex );

		}
	}

	/*
	 * 
	 * I need a function that can combine my unique files with output (winners)
	 * in order to put GIs with the short-ids that I'm using. Other ideas:
	 * cut off the column of short-ids from the blast.uniques file and then use
	 * that column in a grep (-w -f) script to pull the GIs out of the 'winner' file (e.g. all16S.nominscore)
	 */
	private synchronized static String[][] findGIsForFiles( String filenamePattern, String sourcedir, String fileWithGIs, 
					int shortIdIndex, int giFileGiIndex, int giFileShortIdIndex ){

		BufferedReader reader, winners;

		Hashtable<String,String> pairs = new Hashtable<String,String>();

		try{

			TaxaLogger.logInfo( logger, "Using source-dir: " + sourcedir, null, null );
			File dir = new File( sourcedir );
			class RegexFilter implements FilenameFilter{
				java.util.regex.Pattern pattern;

				public RegexFilter(String regex) {
					pattern = java.util.regex.Pattern.compile(regex);
				}

				public boolean accept(File dir, String name) {
					return pattern.matcher( new File(name).getName() ).matches();
				}
			};

			winners = new BufferedReader( new InputStreamReader(
							new FileInputStream( fileWithGIs ), "UTF-8" ));

			String line; String[] fields; 
			while(( line = winners.readLine()) != null ){
				fields = line.split("\t");
				pairs.put( fields[giFileShortIdIndex], fields[giFileGiIndex] );
			}

			TaxaLogger.logInfo( logger, "Intermediate file has: " + pairs.size() + " records", null, null );
			TaxaLogger.logInfo( logger, "Using regex: " + filenamePattern, null, null );

//
// list of the files that fit our regex (first spec use: the short-unique files used for alignment)
			String shortId;
			String[] files = dir.list( new RegexFilter( filenamePattern ));
			for( String file : files ){

				TaxaLogger.logInfo( logger, "Reviewing file: " + file , null, null );
				file = new StringBuffer( sourcedir ).append('/').append( file ).toString();

//
// read the winner-file, parse out the GIs by index and return them.
				Vector<String[]> tmp = new Vector<String[]>();
				reader = new BufferedReader( new InputStreamReader( new FileInputStream( file ), "UTF-8" ));

				while(( line = reader.readLine()) != null ){
					fields = line.split("\t");
					shortId = fields[shortIdIndex];

					if( pairs.containsKey( shortId )){
						tmp.add( new String[]{ pairs.get( shortId ), shortId } );

					} else {
						TaxaLogger.logError( logger, "Short-id: " + shortId + " not in your intermediate file." , null, null );

						System.err.println( "Unable to create fasta file for file: " + file + " since the shortId: " + shortId + " is not in the intermediate file.");
						System.err.println( "Exiting now." );
						System.exit(1);
					}
				}

				String[][] result = new String[tmp.size()][2];
				getFullLengthSequence( file.concat(".fetch.fas"), tmp.toArray( result ));
			}

		} catch( IOException ioEx ){
			ioEx.printStackTrace( System.err );
		}

		return null;
	}

	/*
	 * 
	 * this function expects a tab-delimited file with 'giIndex' being the gi
	 * and 'shortIdIndex' being the index of the short-id
	 */
	private synchronized static String[][] readGIs( String filename, int giIndex, int shortIdIndex ){

		BufferedReader reader;
		Vector<String[]> tmp = new Vector<String[]>();

		try{

//
// read the source-file, parse out the GIs by index and return them.
			reader = new BufferedReader( new InputStreamReader( new FileInputStream( filename ), "UTF-8" ));

			String line; String[] fields;
			while(( line = reader.readLine()) != null ){
				fields = line.split("\t");
				tmp.add( new String[]{ fields[giIndex], fields[shortIdIndex] } );
			}

			String[][] result = new String[tmp.size()][2];
			return tmp.toArray( result );

		} catch( IOException ioEx ){
			ioEx.printStackTrace( System.err );
		}

		return null;
	}

	private synchronized static boolean getFullLengthSequence( String outputfile, String[][] GIs ){
// base url
// 1. http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=nucleotide&id=AB205211&rettype=gbc&retmode=xml

		OutputStreamWriter osw = null;
		BufferedReader reader = null;
		BufferedWriter writer = null;

		try{

	        writer = new BufferedWriter(
	        		new OutputStreamWriter( new FileOutputStream( outputfile, false), "UTF-8"));

			TaxaLogger.logDebug( logger, "Sleeping 200ms between requests per ncbi request.", TaxaLogger.getTime(), null );
			TaxaLogger.logInfo( logger, "Outputting to file: " + outputfile, TaxaLogger.getTime(), null );

			URLConnection cnxn;
			URL url = new URL("http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?");

			//for( Enumeration<String> e = GIs.keys(); e.hasMoreElements(); ){
			for( String[] pair : GIs ){
//
// the key is the gi; the value is the actual short-id;
				String gi = pair[0];
				String shortid = pair[1];

				//System.out.println("retrieving gi: " + gi );

// give each GI 3-chances to successfully query the website.
				int fail = 0;
				boolean connectException;

				do{

// obey ncbi's request to not overload their server(s) by sleeping 200ms...
					Thread.sleep(200);
					connectException = false;

					try{
						cnxn = url.openConnection();
						cnxn.setDoOutput(true);

						try{
							osw = new OutputStreamWriter(cnxn.getOutputStream());
							osw.write( new StringBuffer( "db=nucleotide&rettype=fasta&retmode=text&id=").append( gi ).toString());
			
						} catch( Exception ex ){
							ex.printStackTrace(System.err);

							TaxaLogger.logError( logger, "Reviewing gi: "  + gi + ". An error occurred: " + ex.getMessage(), TaxaLogger.getTime(), null );
							throw new java.net.ConnectException("Error accessing efetch.");
						}

						osw.flush();
						osw.close();

						reader = new BufferedReader( new InputStreamReader(cnxn.getInputStream(), "UTF-8"));

						String line = null;
						while((( line = reader.readLine()) != null ) && ( !"".equals(line.trim()))){

//
// rewrite the headers to include the short-id
							if( line.startsWith(">")){
								writer.write(
										new StringBuffer(">").append( shortid ).append( "|gi|" ).append( gi ).append("|").toString()); //desc.trim()).toString());
						        writer.write('\n');

							} else if( line.trim().startsWith("<html>") || line.trim().startsWith("<?xml")){
// make sure that the line isn't null but that it also isn't just a blank html doc.
								throw new Exception("Html page for some reason?!.");

							} else {
								writer.write( line );
						        writer.write('\n');
							}
						}

						if( fail > 0 ){
							TaxaLogger.logInfo(logger, "Retrieved taxonomic information on retry for GI: " + gi, TaxaLogger.getTime(), null );
						}

					} catch( java.net.ConnectException con ){

						connectException = true;

						fail++;
						if( fail == 5 ){ 
							con.printStackTrace(System.err);
							TaxaLogger.logError( logger,"Error occurred while retrieving taxonomic info: " + gi + ". Error in connecting 5x.\n" +
									con.getMessage(), TaxaLogger.getTime(), null );

						} else {
							Thread.sleep(2000); // sleep 1.5 second between redos.
						}

					} catch( Exception ex ){

						connectException = true;

						fail++;
						if( fail == 5 ){ 
							TaxaLogger.logError( logger, "Error occurred while retrieving taxonomic info: " + gi + ". Error in connecting 5x.\n" +
									ex.getMessage(), TaxaLogger.getTime(), null );

						} else {
							Thread.sleep(2000); // sleep 1 second between attempts.
						}

						ex.printStackTrace(System.err);
					}

				} while( connectException && fail < 5 );

				if( connectException ){}
			}

	        writer.flush();
	        writer.close();

			return true;

		} catch( MalformedURLException malEx ){
			malEx.printStackTrace(System.err);
			TaxaLogger.logError( logger, "Malformed URL: " + malEx.getMessage(), TaxaLogger.getTime(), null );
			return false;

			
		} catch( IOException ioEx ){
			ioEx.printStackTrace(System.err);
			TaxaLogger.logError( logger, "(io) exception: " + ioEx.getMessage(), TaxaLogger.getTime(), null );
			return false;

/*
			connectException = true;

			fail++;
			if( fail == 3 ){ 
				TaxaLogger.logError( logger, "Error occurred while retrieving taxonomic info: " + gi + ". Error in connecting 3x.\n" +
								ioEx.getMessage(), TaxaLogger.getTime(), null );
				
			} else {
				Thread.sleep(1000); // sleep 1 second between attempts.
			}*/

		} catch( InterruptedException ex ){
			ex.printStackTrace(System.err);
			TaxaLogger.logError( logger, "Interrupted Exception: " + ex.getMessage(), TaxaLogger.getTime(), null );
			ex.printStackTrace(System.err);
			return false;

		} finally {}
	}
}
