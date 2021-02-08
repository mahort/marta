package marta.taxa;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;

import org.apache.log4j.Logger;

public class TaxaDatabase {

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

	public static synchronized Hashtable<String,Taxonomy> getTaxonomicInformation( String shortid, String[][] hits ){
// pull out the ids and ignore the scores.
		String[] GIs = new String[hits.length]; int i = 0;
		for( String[] hit : hits ){
			GIs[i++] = ( hit[0] ); // ignore the score<tab>evalue pair
		}

		return getTaxonomicInformation( shortid, GIs );
	}

	public static synchronized Hashtable<String,Taxonomy> getTaxonomicInformation( String shortid, String[] GIs ){

        Connection cnxn = null;
        PreparedStatement prepTaxaId = null, prepRank = null, prepName = null;
        Hashtable<String,Taxonomy> taxa = new Hashtable<String, Taxonomy>();

        try {

// there's a bug here:
// if the user doesn't find the tax_id in the database, we report the parsed description from the 
// blast results, but it isn't clear to the user that's where the data came from. Perhaps report it differently somehow... ?
// 

            Class.forName( "com.mysql.jdbc.Driver" ).newInstance();
            cnxn = DriverManager.getConnection( dburl, dbUserId, dbPassword );

            prepTaxaId = cnxn.prepareStatement( "select taxonid from giToTaxonId where gi = ?");
            prepRank = cnxn.prepareStatement("select parenttaxonid, rank from nodes where taxonid = ?" );
            prepName = cnxn.prepareStatement("select name from names where class = 'scientific name' AND taxonid = ?");

        	for( String gi : GIs ){
//
// the gi will claim another hashtable with taxaIds and ranks stored within...
        		String taxaId = null, parentTaxId = null, rank = null, taxon = null;

            	prepTaxaId.setString( 1, gi );
        		ResultSet rsTaxaId = prepTaxaId.executeQuery(); // here we get our gi's Taxa_Id.

        		if( rsTaxaId.next()){

        			taxaId = rsTaxaId.getString("taxonid");
        			if( Integer.parseInt( taxaId ) != 0 ){

        				Hashtable<String,String[]> ranks = new Hashtable<String,String[]>();
        				boolean enumerated = false;
//
// loop to find all of the ranks.
            			do{
    	        			prepRank.setString( 1, taxaId );
    	        			ResultSet rsRanks = prepRank.executeQuery(); // queries nodes for ranks and parent_tax_ids...

    	        			if( rsRanks.next()){
    	        				enumerated = true;

    	        				parentTaxId = rsRanks.getString("parenttaxonid");
    	        				rank = rsRanks.getString("rank");

    	        				prepName.setString(1, taxaId);
    	        				ResultSet rsNames = prepName.executeQuery();
    	        				rsNames.next();
    	        				taxon = rsNames.getString("name");

//
// now we know the parent_tax_id and rank and taxaId.
    	        				ranks.put( rank, new String[]{ taxon, taxaId });

    	        			} else {
    	        				System.err.println("Error in taxonomy database. No record for taxon: " + taxaId + " impacts gi: " + gi + " and read: " + shortid );
    	        				TaxaLogger.logError( logger, "Error in taxonomy database. No record for taxon: " + taxaId + " impacts gi: " + gi + " and read: "+ shortid, TaxaLogger.getTime(), shortid );
    	        			}

// keep this here, as it is (a.) necessary for subsequent rounds and (b.) helpful for 'resetting' the taxaId to null when
// there are no records for the taxon_id found in the ids table (which for some reason, actually happens).
    	        			taxaId = parentTaxId;

            			} while(( taxaId != null ) && ( Integer.parseInt( taxaId ) != 1 ));

            			if( enumerated ){
            				taxa.put( gi, new Taxonomy( ranks, omitPathovar ));
            			} // otherwise, this gi won't be represented in the return-set

        			} else {
        				TaxaLogger.logError( logger, "Unknown taxonomy for gi: " + gi + " impacts read (short) id: "+ shortid, TaxaLogger.getTime(), shortid );
        				System.err.println( "Unknown taxonomy for gi: " + gi + " impacts read (short) id: " + shortid );
        			}

        		} else {
    				TaxaLogger.logError( logger, "A disconnect between your blastall files and taxonomic database impacts gi: " + gi + " and read (short) id: "+ shortid, TaxaLogger.getTime(), shortid );
        			System.err.println( "A disconnect between your (updated) blastall files and (possibly antiquated) taxonomic database impacts GI: " + gi + " and read: " + shortid );
        		}
        	}

        	return taxa;

        } catch( SQLException sqlEx ){
        	sqlEx.printStackTrace( System.err );
        	TaxaLogger.logError(logger, "SQL Error occurred: " + sqlEx.getMessage(), TaxaLogger.getTime(), shortid );
            return null;

        } catch( Exception ex ){
        	ex.printStackTrace(System.err);
        	TaxaLogger.logError(logger, "An error occurred while retrieving records from the database: " + ex.getMessage(), TaxaLogger.getTime(), shortid );
            return null;

        } finally {
        }
	}
}
