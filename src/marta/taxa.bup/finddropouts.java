package marta.taxa;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.log4j.Logger;

public class finddropouts {

	static Logger logger = Logger.getLogger("marta.taxa");

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		boolean forked = false, multisets = false;

		String sourcedir = null;
		String inputFile = null; //, outputFile = null
		String groupId = null;

		if( args.length > 0 ){

			StringBuffer options = new StringBuffer();
			for( int i = 0; i < args.length; i++ ){
				options.append(args[i]).append(" ");

			} options.append(" "); // one terminal space to match on for the last option.

			String arg = options.toString();
			forked = ( arg.indexOf("-fork") != -1 );
			multisets = ( arg.indexOf("-multisets") != -1 );
			if( multisets ){ forked = true; }

			if( arg.indexOf("-src=") != -1){
				sourcedir = arg.substring(arg.indexOf("-src=") + "-src=".length(), arg.indexOf(" ",arg.indexOf("-src=")));

				java.io.File f = new java.io.File(sourcedir);
				if( !f.exists()){
					System.out.println("Source directory: "+ sourcedir + " does not exist");
					System.out.println("-------------------------------------------------");
					System.exit(1);
				}
			}
/*
			if( arg.indexOf("-o=") != -1){
				outputFile = arg.substring(arg.indexOf("-o=") + "-o=".length(), arg.indexOf(" ",arg.indexOf("-o=")));

			} else {
				outputFile = "dropouts.txt";
			}*/

			if( arg.indexOf("-i=") != -1){
				inputFile = arg.substring(arg.indexOf("-i=") + "-i=".length(), arg.indexOf(" ",arg.indexOf("-i=")));
			}

			if( arg.indexOf("-group=") != -1){
				groupId = arg.substring( arg.indexOf("-group=") + "-group=".length(), arg.indexOf(" ",arg.indexOf("-group=")));
			}

		} else {
			System.out.println("Usage: marta.taxa.finddropout -src= -fork -o= -i= -group=");
			System.exit(1);
		}

		logger.info("Using source directory: " + sourcedir );

		if( !forked ){
			findDropouts( sourcedir, inputFile );

		} else {
			findDropoutsInTheForks( !multisets, sourcedir, inputFile, groupId );
		}
	}

	/*
	 * 
	 */
	private static boolean findDropoutsInTheForks( boolean justOneSet, String directory, String uniqueSequencesFile, String groupId ){

		BufferedReader reader = null;
		BufferedWriter writer = null;

		try{

			Hashtable<String,String> uniqs = new Hashtable<String,String>();

			reader = new BufferedReader( new InputStreamReader( 
					new FileInputStream( new StringBuffer( uniqueSequencesFile ).toString()), "UTF-8"));

			String line; String[] fields;
			while(( line = reader.readLine()) != null ){
				fields = line.split("\t");

//
// after separating the tab-delimited ids and sequences, add them to this hashtable...
				uniqs.put( fields[0], fields[1] );

			} reader.close();


			class GroupFilter implements FileFilter {

				String groupid;
				public GroupFilter( String groupId ){
					groupid = groupId;
				}

				public boolean accept(File file){

					try{
						System.out.println( file.getCanonicalPath());
						if( file.isDirectory() & 
								new File(file.getCanonicalPath().concat("/winner.").concat(
										( null == groupid ? "nogroup" : groupid.trim()))).exists()){

							return true;

						} else { return false; }


					} catch( java.io.IOException ioEx ){
						ioEx.printStackTrace(System.out);
						return false;
					}
				}
			};

			logger.info("Source file has: " + uniqs.size() + " sequences.");

			FilenameFilter filterOverSets = new FilenameFilter(){
				public boolean accept(File f, String name){
					return(f.isDirectory() && name.startsWith("set"));
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

			String key;
			File dir = new File( directory );

			if( justOneSet ){
				File[] forks = dir.listFiles(filterForForks);
				for( File fork : forks ){
					File[] files = fork.listFiles( new GroupFilter( groupId ));
					for( File file : files ){
//
//round up the votes.
						key = file.getName();
						if( uniqs.containsKey( key )){
							uniqs.remove( key );
						}
					}
				}

			} else {
				File[] sets = dir.listFiles(filterOverSets);
				for( File set : sets ){
	
					File[] forks = set.listFiles(filterForForks);
					for( File fork : forks ){
						File[] files = fork.listFiles( new GroupFilter( groupId ));
						for( File file : files ){
//
//round up the votes.
							key = file.getName();
							if( uniqs.containsKey( key )){
								uniqs.remove( key );
							}
						}
					}
				}
			}

			logger.info("Writing retries file now with: " + uniqs.size() + " sequences.");
			String retries = uniqueSequencesFile.substring(0,uniqueSequencesFile.lastIndexOf("/"));
			retries = retries.concat("/retries.txt");

			writer = new BufferedWriter(
					new OutputStreamWriter(
							new FileOutputStream( retries, false ), "UTF-8"));

			Enumeration<String> e = uniqs.keys();
			while( e.hasMoreElements()){
				key = e.nextElement();
				writer.write( key );
				writer.write('\t');
				writer.write( uniqs.get( key ));
				writer.write('\n');
			}

			writer.flush();
			writer.close();

			logger.info("Retries file is written.");
			return true;

		} catch( IOException ioEx ){
			ioEx.printStackTrace(System.out);
			logger.error( "Find-dropouts: " + ioEx.getMessage());
			return false;

		} finally {
		}
	}

	/*
	 * 
	 */
	private static boolean findDropouts( String directory, String uniqueSequencesFile ){

		BufferedReader reader = null;
		BufferedWriter writer = null;
		try{

			Hashtable<String,String> uniqs = new Hashtable<String,String>();

			reader = new BufferedReader( new InputStreamReader( 
					new FileInputStream( new StringBuffer( uniqueSequencesFile ).toString()), "UTF-8"));

			String line; String[] fields;
			while(( line = reader.readLine()) != null ){
				fields = line.split("\t");

//
// after separating the tab-delimited ids and sequences, add them to this hashtable...
				uniqs.put( fields[0], fields[1] );

			} reader.close();


			FileFilter filterWinners = new FileFilter(){

				public boolean accept(File file){


					try{
						System.out.println( file.getCanonicalPath());
						if( file.isDirectory() & 
								new File(file.getCanonicalPath().concat("/winner.txt")).exists() &
								new File(file.getCanonicalPath().concat("/besthits.txt")).exists()){
							return true;

						} else { return false; }

					} catch( java.io.IOException ioEx ){
						ioEx.printStackTrace(System.out);
						return false;
					}
				}
			};

			logger.info("Source file has: " + uniqs.size() + " sequences.");
			String key;
			File dir = new File(directory);
			File[] files = dir.listFiles(filterWinners);

			logger.info("Directory has: " + files.length + " folders.");
			for( File file : files ){
// round up the votes.
				//System.out.println("File found: " + file.getName());
				key = file.getName();
				if( uniqs.containsKey( key )){
					uniqs.remove( key );
				}
			}

			logger.info("Writing retries file now with: " + uniqs.size() + " sequences.");
			String retries = uniqueSequencesFile.substring(0,uniqueSequencesFile.lastIndexOf("/"));
			retries = retries.concat("/retries.txt");

			writer = new BufferedWriter(
					new OutputStreamWriter(
							new FileOutputStream( retries, false ), "UTF-8"));

			Enumeration<String> e = uniqs.keys();
			while( e.hasMoreElements()){
				key = (String)e.nextElement();
				writer.write( key );
				writer.write('\t');
				writer.write( uniqs.get( key ));
				writer.write('\n');
			}

			writer.flush();
			writer.close();

			logger.info("Retries file is written.");
			return true;

		} catch( IOException ioEx ){
			ioEx.printStackTrace(System.out);
			logger.error( "Find-dropouts: " + ioEx.getMessage());
			return false;

		} finally {
		}
	}
}
