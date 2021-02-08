package marta.taxa;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.Logger;

public class TaxaToFullTaxonomy {

	static Logger logger = Logger.getLogger("marta.taxa");
	static String dbDriver = null, dburl = null, dbUserId = null, dbPassword = null, fileOfTaxa = null;
	static boolean omitPathovar = false;

	static {

	    java.util.Properties props = new java.util.Properties();
	    try {

	    	props.load( new java.io.FileInputStream("marta.properties"));
	    	dbDriver = props.getProperty("dbdriver");
	    	dburl = props.getProperty("dburl");
	    	dbUserId = props.getProperty("dbuserid");
	    	dbPassword = props.getProperty("dbpassword");

	    	fileOfTaxa = props.getProperty("getfulltaxonomy");

	    	if( props.containsKey("trimpathovarassignment")){
	    		omitPathovar = Boolean.parseBoolean( props.getProperty("trimpathovarassignment"));
	    	}

	    	logger.info( "Set properties. Omit pathovar: " + omitPathovar );

	    } catch( IOException e ){
	       e.printStackTrace();
	       logger.error("Error loading db properties. Make sure that marta.properties is in the classpath and that the properties are correctly labeled.");
	    }
	}

	public static void main(String[] args){

// we should go get the taxonomic information here...
// actually:
// 1. http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=nucleotide&id=AB205211&rettype=gbc&retmode=xml
// 1.a. <INSDQualifier_value>taxon:70189</INSDQualifier_value>
// 1.b. organism names for voting.
// THEN AND ONLY THEN ON REVIEW OF NAMES...
// 2. http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=taxonomy&id=70189&report=xml
// 2.a. taxonomic info for winner only
        Connection cnxn = null;
        PreparedStatement prepSpuh = null, prepLike = null, prepRank = null, prepName = null;
        Hashtable<String,Taxonomy> taxa = new Hashtable<String, Taxonomy>();

        BufferedReader reader = null;

        try{

            Class.forName( "com.mysql.jdbc.Driver" ).newInstance();
            cnxn = DriverManager.getConnection( dburl, dbUserId, dbPassword );

            prepSpuh = cnxn.prepareStatement( "select taxonid from names where name = ?" );
            prepLike = cnxn.prepareStatement( "select taxonid from names where name like ? ");
            prepRank = cnxn.prepareStatement( "select parenttaxonid, rank from nodes where taxonid = ?" );
            prepName = cnxn.prepareStatement( "select name from names where class = 'scientific name' AND taxonid = ?" );

			reader = new BufferedReader( new InputStreamReader( new FileInputStream( fileOfTaxa ), "UTF8"));

			Hashtable<String,String> dropouts = new Hashtable<String,String>();

			ResultSet rsSpuh = null;
			String line, taxonId, parentTaxonId = null, rank, taxon;
			while((line = reader.readLine()) != null ){


				if( line.indexOf("%") != -1 ){
					prepLike.setString(1, line.trim());
					rsSpuh = prepLike.executeQuery();

				} else {
					prepSpuh.setString( 1, line.trim());
					rsSpuh = prepSpuh.executeQuery();
				}

				if( !rsSpuh.next()){
					TaxaLogger.logError( logger, "A disconnect between your blastall files and taxonomic database impacts spuh: " + line, TaxaLogger.getTime(), line );
	    			System.err.println( "A disconnect between your (updated) blastall files and (possibly antiquated) taxonomic database impacts spuh: " + line );
	    			dropouts.put( new StringBuffer("S").append(dropouts.size()).toString(), line.concat("\tspuh not found"));

				} else {

					taxonId = rsSpuh.getString("taxonid");

					if( Integer.parseInt( taxonId ) == 0 ){
	    				TaxaLogger.logError( logger, "Unknown taxonomy for spuh: " + line, TaxaLogger.getTime(), line );
	    				System.err.println( "Unknown taxonomy for spuh: " + line );
		    			dropouts.put( new StringBuffer("S").append(dropouts.size()).toString(), line.concat("\ttaxonid is zero"));

					} else {
        				Hashtable<String,String[]> ranks = new Hashtable<String,String[]>();
        				boolean enumerated = false;

//
// get the upper level ranks by looping.
        				do{
		        			prepRank.setString( 1, taxonId );
		        			ResultSet rsRanks = prepRank.executeQuery(); // queries nodes for ranks and parent_tax_ids...
	
		        			if( rsRanks.next()){
		        				enumerated = true;
	
		        				parentTaxonId = rsRanks.getString("parenttaxonid");
		        				rank = rsRanks.getString("rank");
	
		        				prepName.setString(1, taxonId);
		        				ResultSet rsNames = prepName.executeQuery();
		        				rsNames.next();
		        				taxon = rsNames.getString("name");

//
// now we know the parent_tax_id and rank and taxaId.
		        				ranks.put( rank, new String[]{ taxon, taxonId });

		        			} else {
		        				System.err.println("Error in taxonomy database. No real record for taxon: " + taxonId + " or spuh: " + line );
		        				TaxaLogger.logError( logger, "Error in taxonomy database. No record for taxon: " + taxonId + " or spuh: " + line, TaxaLogger.getTime(), null );
		        			}

//
// keep this here, as it is (a.) necessary for subsequent rounds and (b.) helpful for 'resetting' the taxaId to null when
// there are no records for the taxon_id found in the ids table (which for some reason, actually happens).
		        			taxonId = parentTaxonId;

	        			} while(( taxonId != null ) && ( Integer.parseInt( taxonId ) != 1 ));

        				TaxaLogger.logError( logger, "Found taxonomy for spuh: " + line, TaxaLogger.getTime(), line );
        				System.out.println( "Found taxonomy for spuh: " + line );

	        			if( enumerated ){
	        				taxa.put( line.trim(), new Taxonomy( ranks, omitPathovar ));

	        			} else {
	        				// otherwise, this spuh won't be represented in the return-set
			    			dropouts.put( new StringBuffer("S").append(dropouts.size()).toString(), line.concat("\tError in taxonomy database. The taxonid refers to a nonexistent record"));
	        			}
	    			}
	    		}
			}

//
// once we have the hashtable<string,taxonomy> filled with spuhs and their complete taxonomies,
// write the thing out to file.
	        BufferedWriter writer = new BufferedWriter(
	        		new OutputStreamWriter( new FileOutputStream( fileOfTaxa.concat(".full"), false), "UTF-8"));
	        writer.write("id\ttaxon\tfull-taxonomy\n");

	        String spuh;
	        java.util.Enumeration<String> e = taxa.keys();
	        while( e.hasMoreElements()){
	        	spuh = e.nextElement();
	        	writer.write( spuh );
	        	writer.write( '\t' );
	        	writer.write( taxa.get( spuh ).getFullTaxonomy(":"));
	        	writer.write('\n');
	        }

	        e = dropouts.keys();
	        while( e.hasMoreElements()){
	        	spuh = e.nextElement();
	        	writer.write( dropouts.get( spuh ))	;
	        	writer.write('\n');
	        }

	        writer.flush();
	        writer.close();

        	return;

        } catch( SQLException sqlEx ){
        	sqlEx.printStackTrace( System.err );
        	TaxaLogger.logError(logger, "SQL Error occurred: " + sqlEx.getMessage(), TaxaLogger.getTime(), null );
            return;

        } catch( Exception ex ){
        	ex.printStackTrace(System.err);
        	TaxaLogger.logError(logger, "An error occurred while retrieving records from the database: " + ex.getMessage(), TaxaLogger.getTime(), null );
            return;

        } finally {}
	}
}
