package marta.taxa;

import org.apache.log4j.Logger;

public class HitDescription {

	private static Logger logger = Logger.getLogger("marta.taxa");

	private final int COLUMN_ID = 2;
	private final int COLUMN_PERC_IDENTITY = 3;
	private final int COLUMN_ALIGN_LENGTH = 4;
	private final int COLUMN_MINIMUM_EVALUE = 11;
	private final int COLUMN_BITSCORE = 12;

	private String id;
	private String shortId;
	private String percentageIdentity;
	private int length;
	private double minimumEvalue;
	private double bitscore;
	private String full;
	
	public HitDescription( String shortId, String description ){
		this.shortId = shortId;
		this.full = description;

		String[] fields = full.replaceAll("  +", "\t").split("\t");
		String[] ids = fields[COLUMN_ID-1].split("\\|")[1].split("\\."); //[0]; // pipes separate the ids, but there are often dots inside the names THAT ARE NOT INCLUDED IN THE ALIGNMENT NAMES (to denote strand). e.g. emb|AJ242887.1|RSP242887

		this.id = ids[0];
		this.percentageIdentity = fields[COLUMN_PERC_IDENTITY - 1];
		this.length = Integer.parseInt( fields[COLUMN_ALIGN_LENGTH - 1]);

// minimum E-value and bitscore.
		String tmp = fields[COLUMN_MINIMUM_EVALUE - 1];
		if( tmp.startsWith("e")){ tmp = "1".concat(tmp); }
		this.minimumEvalue = Double.parseDouble(tmp );
		this.bitscore = Double.parseDouble( fields[ COLUMN_BITSCORE - 1] );

		TaxaLogger.logDebug( logger, "Minimum E-value: " + this.minimumEvalue, TaxaLogger.getTime(), shortId );
		TaxaLogger.logDebug( logger, "Bitscore: " + this.bitscore, TaxaLogger.getTime(), shortId );
		TaxaLogger.logDebug( logger, "Length: " + this.length, TaxaLogger.getTime(), shortId );
		TaxaLogger.logDebug( logger, "Percentage Identity: " + this.percentageIdentity, TaxaLogger.getTime(), shortId );
	}

//	gi
	public String getId() {
		return this.id;
	}

	public void setId(String id) {
		this.id = id;
	}

//	short-id
	public String getShortId(){
		return this.shortId;
	}

	public void setShortId( String shortId ){
		this.shortId = shortId;
	}

//	perc-identity (%age similarity b/w target and this hit for the given overlap);
	public String getPercentageIdentity(){
		return this.percentageIdentity;
	}

//	length itself.
	public int getLength(){
		return this.length;
	}

//	minimum e-value
	public double getMinimumEvalue(){
		return this.minimumEvalue;
	}

//	bitscore
	public double getBitscore(){
		return this.bitscore;
	}
}
