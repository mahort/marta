package marta.taxa;

import org.apache.log4j.Logger;

public class OldHitDescription {

	private static Logger logger = Logger.getLogger("marta.taxa");

	private static Logger getLogger(){
		return logger;
	}

	boolean updatedTaxonomicInformation;

	private String shortId;
	private String id;
	private String taxonid;
	private String genus;
	private String species;
	private double score;
	private String strand = null;
	private String full;
	
	public OldHitDescription( String shortId, String description ){
		this.shortId = shortId;
		this.full = description;

		String[] fields = full.replaceAll("  +", "\t").split("\t");
		String[] ids = fields[0].split("\\|")[1].split("\\."); //[0]; // pipes separate the ids, but there are often dots inside the names THAT ARE NOT INCLUDED IN THE ALIGNMENT NAMES (to denote strand). e.g. emb|AJ242887.1|RSP242887
		if( ids.length > 1 ){ // is there always strand information?
			strand = ids[1];

		}

		this.id = ids[0];
		this.genus = fields[1].trim().split(" ")[0]; // the string[] value has two values. Ignoring 'Uncultured' elements, these will normally be {gen,spp}

		if( fields[1].trim().split(" ").length > 1 ){
			this.species = fields[1].trim().split(" ")[1];
		}

		String tmp = fields[fields.length - 1];
		if( tmp.startsWith("e")){ tmp = "1".concat(tmp); }
		this.score = Double.parseDouble(tmp );
	}

	public String getShortId(){
		return shortId;
	}

	public void setShortId( String shortId ){
		this.shortId = shortId;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public boolean isTaxonomicInformationUpdated(){
		return this.updatedTaxonomicInformation;
	}

	public void setTaxonomicInformationUpdated( boolean updated ){
		this.updatedTaxonomicInformation = updated;
	}

	public String getTaxon(){
		return new StringBuffer( genus ).append(
				( species == null ? "" : new StringBuffer("\t").append( species ).toString())).toString();
	}

	public String toProperCase( String text ){

		char[] c = text.toLowerCase().toCharArray();
		c[0] = Character.toUpperCase(c[0]);
		return( new String(c));
	}

	public void setTaxon( String taxon ){
		if( taxon == null ){
			getLogger().info("taxon for element: " + getShortId() + " is null (element " + id );
			return;
		}

		this.updatedTaxonomicInformation = true;
		this.genus = null;
		this.species = null;

		String[] fields;
		String prefix;
// 
//
		fields = taxon.split(" ");
		if( taxon.toLowerCase().trim().startsWith("uncultured")){ // go three deep just in case...
			switch( fields.length ){
			case 1:
				genus = "Uncultured";
				break;

			case 3:
				species = fields[2].toLowerCase();

			case 2:
			default:
// get the proper case set for comparison...
				prefix = toProperCase(fields[0]);
				genus = toProperCase(fields[1]);

				genus = new StringBuffer(prefix).append(" ").append(genus).toString();
				break;
			}

		} else {
			switch( fields.length ){
			case 2:
				species = fields[1].toLowerCase();
			case 1:
			default:
				genus = toProperCase(fields[0]);
				break;
			}
		}

		if( null == species || "sp".equals( species )){ species = "sp."; }
	}

	public String getGenus() {
		return genus;
	}

	public void setGenus(String genus) {
		this.genus = genus;
	}

	public String getSpecies(){
		return species;
	}

	public void setSpecies(String species){
		this.species = species;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public String getStrand() {
		return strand;
	}

	public void setStrand(String strand) {
		this.strand = strand;
	}

	public String getFullDescription(){
		return full;
	}
	
	public void setFullDescription(String full){
		this.full = full;
	}

	public String getTaxonid(){
		return taxonid;
	}

	public void setTaxonid( String taxonid ){
		this.taxonid = taxonid;
	}

	private String rank;
	public String getRank(){
		return rank;
	}

	public void setRank( String rank ){
		this.rank = rank;
	}

	public void print(){
		System.out.println("id: " + id );
		System.out.println("score: " + score );
		System.out.println("genus: " + genus );
		System.out.println("species: " + species );
		System.out.println( "...................." );
	}
}
