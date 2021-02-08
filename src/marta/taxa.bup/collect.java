package marta.taxa;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import java.util.Scanner;
import java.util.Vector;

import org.apache.log4j.Logger;

public class collect {

	static Logger logger = Logger.getLogger("marta.taxa");

	public static void main(String[] args){

// *** very important that we push back on AssignTaxa and/or revote and make sure that the 'groupid' is one and only one word!!!

		boolean forked = false, oneset = false;

		String groupId = null;
		String outputFile = null;
		String sourcedir = null;

		if( args.length > 0 ){

			StringBuffer options = new StringBuffer();
			for( int i = 0; i < args.length; i++ ){
				options.append(args[i]).append(" ");

			} options.append(" "); // one terminal space to match on for the last option.

			String arg = options.toString();
			forked = ( arg.indexOf("-fork") != -1 );
			oneset = ( arg.indexOf("-set" ) != -1 );

			if( arg.indexOf("-src=") != -1){
				sourcedir = arg.substring(arg.indexOf("-src=") + "-src=".length(), arg.indexOf(" ",arg.indexOf("-src=")));

				java.io.File f = new java.io.File(sourcedir);
				if( !f.exists()){
					System.out.println("Source directory: "+ sourcedir + " does not exist");
					System.out.println("-------------------------------------------------");
					System.exit(1);
				}
			}


			if( arg.indexOf("-o=") != -1){
				outputFile = arg.substring(arg.indexOf("-o=") + "-o=".length(), arg.indexOf(" ",arg.indexOf("-o=")));

			} else {
				outputFile = "blastAssignments.txt";
			}

			if( arg.indexOf("-group=") != -1){
				groupId = arg.substring( arg.indexOf("-group=") + "-group=".length(), arg.indexOf(" ",arg.indexOf("-group=")));
			}

		} else {
			System.out.println("Usage: marta.taxa.collect -src= -fork -set -o= -group=");
			System.exit(1);
		}

//
// the filter...
		FileFilter filter = new FileFilter(){
			public boolean accept(File f){
				return(f.isDirectory());
			}
		};

		if( args[0].equals("list")){
			chooseGroup( args[1] );

		} else if( forked && !oneset ){

			System.out.println("Collecting a full-set.");
			logger.info("Appending output to file: " + outputFile );

			try{

				FilenameFilter filterOverSets = new FilenameFilter(){
					public boolean accept(File f, String name){
						return(name.startsWith("set"));
					}
				};

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

				File dir = new File( sourcedir );
				File[] sets = dir.listFiles(filterOverSets);

				if( groupId == null ){ // the history files can help us locate the group Id if the user chooses it.
					if( sets.length == 0 ){
						TaxaLogger.logError( logger, "This isn't the source directory. No 'sets' were found.", TaxaLogger.getTime(), null );
						System.err.println("This isn't the source directory that you think it is. No 'sets' were found.");
						System.exit(1);
					}

					for( File set : sets ){
						groupId = chooseGroup( set.getCanonicalPath());
						if( groupId == null ){ // still??
							TaxaLogger.logError( logger, "Cannot collect without a group id. Exiting now.", TaxaLogger.getTime(), null );
							System.exit(1);

						} else {
							break;
						}
					}
				}

				TaxaLogger.logInfo( logger, "User chose group: " + groupId, TaxaLogger.getTime(), null );
				System.out.println("Collecting group-set: " + groupId );

				for( File set : sets ){

					File[] forks = set.listFiles(filterForForks);
					for( File fork : forks ){
						System.out.println("At element: " + fork.getCanonicalPath());
						File[] files = fork.listFiles( filter );

						for( File file : files ){
							TaxaLogger.logDebug(logger, "Collecting: " + file.getCanonicalPath(), "", file.getName());
							collectWinner( file.getCanonicalPath(), file.getName(), outputFile, groupId );
						}
					}
				}
	
			} catch ( Exception ex ){
				TaxaLogger.logError(logger, ex.getMessage(), TaxaLogger.getTime(), null);
				ex.printStackTrace(System.err);
			}

		} else if( oneset ){

			System.out.println("Collecting a single-set.");
			logger.info("Beginning to collect the files for our 'single-set': " + sourcedir );

			try{
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

				File dir = new File( sourcedir );
				File[] forks = dir.listFiles(filterForForks);

				if( groupId == null ){ // the history files can help us locate the group Id if the user chooses it.

					if( forks.length == 0 ){
						TaxaLogger.logError( logger, "This isn't the single-set source directory. No 'forks' were found.", TaxaLogger.getTime(), null );
						System.err.println( "This isn't the single-set source directory. No 'forks' were found." );
						System.exit(1);
					}

					groupId = chooseGroup( sourcedir );
				}

				TaxaLogger.logInfo( logger, "User chose group-set: " + groupId, TaxaLogger.getTime(), null );
				System.out.println("Collecting group: " + groupId );

				for( File fork : forks ){
					File[] files = fork.listFiles( filter );

					System.out.println("At element: " + fork.getCanonicalPath());

					for( File file : files ){
						TaxaLogger.logDebug(logger, "Collecting: " + file.getCanonicalPath(), "", file.getName());
						collectWinner( file.getCanonicalPath(), file.getName(), outputFile, groupId );
					}
				}
	
			} catch ( Exception ex ){
				TaxaLogger.logError(logger, ex.getMessage(), TaxaLogger.getTime(), null);
				ex.printStackTrace(System.err);
			}

		} else {

			logger.info("Collecting files from: " + outputFile );

//
// TODO: make this amenable for collecting the lines for ONLY ONE SET!!!
			try{

				File dir = new File(sourcedir);
				File[] files = dir.listFiles(filter);

				for( File file : files ){
//
// round up the votes...
					System.out.println("name: " + file.getName());
					System.out.println("directory: " + file.getCanonicalPath());
					collectWinner( file.getCanonicalPath(), file.getName(), outputFile, groupId );
				}

			} catch ( Exception ex ){
				ex.printStackTrace(System.out);
			}
		}

		logger.info("Finished collecting from: " + sourcedir );
		System.out.println("Finished collecting reads from: " + sourcedir );
	}

	private static synchronized String chooseGroup( String directory ){

		StringBuffer prompt = new StringBuffer();
		BufferedReader reader = null;

		try{

			reader = new BufferedReader( new InputStreamReader( new FileInputStream( 
							new StringBuffer( directory ).append( "/history.txt").toString()), "UTF-8"));

			String line = null;
			Vector<String> groupIds = new Vector<String>();
			while(( line = reader.readLine()) != null ){ // all except eof.
				if( line.startsWith("groupId")){
					String id = line.split(":")[1].trim();
					if( !groupIds.contains( id )){
						groupIds.add( id );
					}
				}

				prompt.append( line ).append('\n');
			}

			Scanner in = new Scanner( System.in );
			while( true ){
				int choice = 0;
				System.out.println("Please choose the group to collect.");

				for( String group : groupIds ){
					System.out.println(String.valueOf(++choice) + ".) " + group );
	
				} System.out.println( String.valueOf( ++choice ) + ".) describe all." );
	
				System.out.print("> ");
				int response = in.nextInt();
	
				if( response > groupIds.size() ){
					System.out.println(prompt.toString());
					System.out.println("Note: if you used the same group-id multiple times, the last (most-recent) settings were used.");
	
				} else {
	
					return( groupIds.get( --response ));
				}
			}

		} catch( IOException ioEx ){
			TaxaLogger.logError( logger, "IOException: " + ioEx.getMessage(), TaxaLogger.getTime(), null );
			ioEx.printStackTrace(System.err);
		}

		return null;
	}

	/*
	 * this collects the winner (given a 'group' assignment, since winner file is in the format winner.groupid)
	 * and appends it to the specified outputfile. nb: the user can select based on the specified groupid using stdin.
	 */
	private static boolean collectWinner( String directory, String name, String taxaFile, String groupId ) throws IOException {

		BufferedReader reader = null;
		BufferedWriter writer = null;
		try{

			writer = new BufferedWriter(
					new OutputStreamWriter(
							new FileOutputStream( taxaFile, true), "UTF-8"));

			File f = new File(
					new StringBuffer( directory ).append( "/winner.").append( groupId ).toString());

			if( !f.exists()){
				writer.write( name );
				writer.write('\t');
				writer.write( "No taxa information. Likely cause: strange file format. Retry blast.");
				writer.write('\n');
				return false;
			}

			reader = new BufferedReader( new InputStreamReader( new FileInputStream( 
					new StringBuffer( directory ).append( "/winner.").append( groupId ).toString()), "UTF-8"));

			String line = reader.readLine();
			if( null != line ){
				writer.write( line );

			} else {
				writer.write( "Error writing file." );
			}

			writer.write('\n');
			return true;

		} catch( IOException ioEx ){
			ioEx.printStackTrace(System.out);
			logger.error("Error in collecting for target: "+ name);
			return false;

		} finally {
			if( reader != null ){
				reader.close();
			}

			if( writer != null ){
				writer.flush();
				writer.close();
			}
		}
	}
}