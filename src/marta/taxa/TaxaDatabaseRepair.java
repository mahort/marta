package marta.taxa;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;

import org.apache.log4j.Logger;

public class TaxaDatabaseRepair {

	static Logger logger = Logger.getLogger("marta.taxa");
	static String dbDriver = null, dburl = null, dbUserId = null, dbPassword = null;
	static String genera = null;

	static {

	    java.util.Properties props = new java.util.Properties();
	    try {
	    	props.load( new java.io.FileInputStream("dbrepair.properties"));
	    	dbDriver = props.getProperty("dbdriver");
	    	dburl = props.getProperty("dburl");
	    	dbUserId = props.getProperty("dbuserid");
	    	dbPassword = props.getProperty("dbpassword");
	    	genera = props.getProperty("taxa");

	    } catch( IOException e ){
	       e.printStackTrace();
	       logger.error("Error loading db properties. Make sure that marta.properties is in the classpath and that the properties are correctly labeled.");
	    }
	}

	public static void main( String[] args ){

		System.out.println("Considering candidates: " + genera );
        int size = 0;  
        String taxonId = null, goodId = null, feedback = null;

        Connection cnxn = null;
        PreparedStatement prepSpecies = null, prepGenus = null, prepUpdate = null; // prepRank = null, prepName = null;
        Hashtable<String,Taxonomy> results = new Hashtable<String, Taxonomy>();

        try {

            Class.forName( "com.mysql.jdbc.Driver" ).newInstance();
            cnxn = DriverManager.getConnection( dburl, dbUserId, dbPassword );
            cnxn.setAutoCommit( false  );

// nb: some taxa have a species group b/w the spp node, the 'no rank' classifier and the family node.
            prepSpecies = cnxn.prepareStatement(
            		new StringBuffer("select child.taxonid as kidid from nodes ").append(
            				"child inner join nodes parent on parent.taxonid = child.parenttaxonid " ).append(
            				"inner join nodes grandpa on grandpa.taxonid = parent.parenttaxonid left ").append(
            				"join names on child.taxonid = names.taxonid where child.rank = 'species' ").append(
            				"and grandpa.rank = 'family' and parent.rank = 'no rank' and class='scientific ").append(
            				"name' and name not like '% sp.%' and name not like '%environmental%' and name ").append(
            				"not like '%idae %' and name not like '%aceae %' and name not like '% cf. %' and ").append(
            				"name not like '%phage%' and name not like '%virus%' and name not like ").append(
            				"'%uncultured%' and name not like '%unidentified%' and name like ?").toString());

            prepGenus = cnxn.prepareStatement(
            		new StringBuffer("select taxonid from names where name = ? and class='scientific name'").toString());

            prepUpdate = cnxn.prepareStatement(
            		new StringBuffer("update nodes set parenttaxonid = ? where taxonid = ?").toString());

            String[] taxa = genera.split(",");
            for( String genus : taxa ){

// make sure that there is only one genus...
               	prepGenus.setString( 1, genus );
                ResultSet rightGenus = prepGenus.executeQuery();

                if( rightGenus.next()){  
                	rightGenus.beforeFirst();  
                	rightGenus.last();  
                	size = rightGenus.getRow();

                	if( size != 1 ){
                		logger.error("There is than one genus record for : " + genus + " having the value 'scientific name'" );
                		System.err.println("Error: only one genus should be found w/ 'scientific name' class @ the specified genus (" + genus + ").");
                		System.exit(1);
                	}

                	goodId = rightGenus.getString("taxonid");

                	ResultSet ranks = cnxn.createStatement().executeQuery("select count(*) as n from nodes where rank='no rank' and taxonid=" + goodId +";");
                	if( ranks.next()){

                		int count = Integer.parseInt(ranks.getString("n"));
                		if( count > 0 ){
                			cnxn.createStatement().executeUpdate("update nodes set rank='genus' where rank='no rank' and taxonid=" + goodId + ";");
                		}
                	}

                } else {
                	feedback = "Genus: " + genus + " not found.";
                	logger.error( feedback ); System.err.println( feedback );
                }

            	prepSpecies.setString( 1, new StringBuffer( genus ).append("%").toString());
                ResultSet errors = prepSpecies.executeQuery();

                if( errors.next()){  
                	errors.beforeFirst();  
                	errors.last();  
                	size = errors.getRow();  
                }

                feedback = "Number of erroneous records for genus: " + genus + " is: " + size;
                logger.info( feedback ); System.out.println( feedback );

                errors.beforeFirst();

                while( errors.next()){
                	taxonId = errors.getString("kidid");
                	System.out.println("Resetting the parenttaxonid for: " + taxonId + " to: "+ goodId );
                	prepUpdate.setString( 1, goodId );
                	prepUpdate.setString( 2, taxonId );
                	prepUpdate.executeUpdate();
                }
            }

        	cnxn.commit();
        	cnxn.setAutoCommit(true);

        } catch( SQLException sqlEx ){
        	sqlEx.printStackTrace( System.err );

        } catch( Exception ex ){
        	ex.printStackTrace(System.err);

        } finally {}
	}
}
