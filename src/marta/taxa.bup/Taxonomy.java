package marta.taxa;

import java.util.Hashtable;

public class Taxonomy {

	private String superkingdom = null;
	private String superkingdomTaxonid = null;
	private String kingdom = null;
	private String kingdomTaxonid = null;
	private String phylum = null;
	private String phylumTaxonId = null;
	private String taxonclass = null; // class is a reserved keyword, der.
	private String taxonclassId = null;
	private String order = null;
	private String orderTaxonId = null;
	private String family = null;
	private String familyTaxonid = null;
	private String genus = null;
	private String genusTaxonId = null;
	private String species = null;
	private String speciesTaxonId = null;

	private final String SUPERKINGDOM = "superkingdom";
	private final String KINGDOM = "kingdom";
	private final String PHYLUM = "phylum";
	private final String CLASS = "class";
	private final String ORDER = "order";
	private final String FAMILY = "family";
	private final String GENUS = "genus";
	private final String SPECIES = "species";

	public Taxonomy( Hashtable<String,String[]> ranks, boolean omitpathovar ){

		String[] info;
		if( ranks.containsKey( SUPERKINGDOM )){
			info = ranks.get( SUPERKINGDOM );
			this.setSuperkingdom ( info[0] );
			this.setSuperkingdomTaxonid ( info[1] );
		}

		if( ranks.containsKey( KINGDOM )){
			info = ranks.get( KINGDOM );
			this.setKingdom( info[0] );
			this.setKingdomTaxonid( info[1] );
		}

		if( ranks.containsKey( PHYLUM )){
			info = ranks.get( PHYLUM );
			this.setPhylum( info[0] );
			this.setPhylumTaxonId( info[1] );
		}

		if( ranks.containsKey( CLASS )){
			info = ranks.get( CLASS );
			this.setTaxonClass( info[0] );
			this.setTaxonclassId( info[1] );
		}

		if( ranks.containsKey( ORDER )){
			info = ranks.get( ORDER );
			this.setOrder( info[0] );
			this.setOrderTaxonId( info[1] );
		}

		if( ranks.containsKey( FAMILY )){
			info = ranks.get( FAMILY );
			this.setFamily( info[0] );
			this.setFamilyTaxonid(  info[1] );
		}

		if( ranks.containsKey( GENUS )){
			info = ranks.get( GENUS );
			this.setGenus( info[0] );
			this.setGenusTaxonId( info[1] );
		}

		if( ranks.containsKey( SPECIES )){
			info = ranks.get( SPECIES );
			this.setSpecies( info[0], omitpathovar );
			this.setSpeciesTaxonId( info[1] );
		}
	}
/*
	public String getSpecificValue( int level ){
		/*switch( level ){
		case PHYLUM:
		case CLASS:
		case ORDER:
		case FAMILY:
		case GENUS:
		case SPECIES:
		}*

		return null;
	}*/

	public String getSuperkingdom() {
		return superkingdom;
	}

	public void setSuperkingdom(String superkingdom) {
		this.superkingdom = superkingdom;
	}

	public String getKingdom() {
		return kingdom;
	}

	public void setKingdom(String kingdom) {
		this.kingdom = kingdom;
	}

	public String getPhylum() {
		return phylum;
	}

	public void setPhylum(String phylum) {
		this.phylum = phylum;
	}

	public String getTaxonClass(){
		return taxonclass;
	}

	public void setTaxonClass( String taxonclass ){
		this.taxonclass = taxonclass;
	}

	public String getOrder() {
		return order;
	}

	public void setOrder(String order) {
		this.order = order;
	}

	public String getFamily() {
		return family;
	}

	public void setFamily(String family) {
		this.family = family;
	}

	public String getGenus() {
		return genus;
	}

	public void setGenus( String genus ){
		this.genus = genus;
	}

	public String getSpeciesAbbreviated(){
		if( species == null ){ return null; }

		if( species.indexOf("sp.") != -1 ){
			return new StringBuffer( species.split("sp\\.")[0].trim()).append(" sp.").toString();

		} return species;
	}

	public String getSpecies() {
		return species;
	}

	public void setSpecies( String species, boolean omitpathovar ){
		
		if( omitpathovar ){
			String[] tmp = species.split(" ");
			if( tmp.length >= 2 ){
				this.species = new StringBuffer( tmp[0] ).append( " " ).append( tmp[1] ).toString();
	
			} else {
				this.species = tmp[0];
			}

		} else {
			this.species = species;
		}
	}

	public String printLineageAndStatus( String gi, String score ){

		String status = null;
		if( null == getGenus() || null == getSpecies() ||
				getSpecies().toLowerCase().startsWith("Uncultur".toLowerCase()) ||
				getSpecies().toLowerCase().startsWith("Unknown".toLowerCase()) ||
				getSpecies().toLowerCase().startsWith("Unidentified".toLowerCase())){

			status = "Uncultured";

		} else {
			status = "Cultured";
		}

		return( new StringBuffer( status ).append('\t').append( score ).append( '\t' ).append( printLineage( gi )).toString());
	}

	public String printLineage( String gi ){

		boolean itIsASpuh = ( this.species == null || this.species.indexOf(" sp.") != -1 );

		return new StringBuffer( 
				( gi == null || "".equals(gi.trim()) ? "" : gi.concat("\t"))).append(
				( this.superkingdom == null ? "" : this.superkingdom.concat("\t"))).append(
				( this.kingdom == null ? "" : this.kingdom.concat("\t"))).append(
				( this.phylum == null ? "" : this.phylum.concat("\t"))).append(
				( this.taxonclass == null ? "" : this.taxonclass.concat("\t"))).append(
				( this.order == null ? "" : this.order.concat("\t"))).append(
				( this.family == null ? "" : this.family.concat("\t"))).append(
				( this.genus == null ? "" : this.genus.concat("\t"))).append(
				( this.species == null ? "" :  ( itIsASpuh ? new StringBuffer( "[" ).append(
						this.species ).append("]").toString() : this.species ))).toString();
	}

	public String getSuperkingdomTaxonid() {
		return superkingdomTaxonid;
	}

	public void setSuperkingdomTaxonid(String superkingdomTaxonid) {
		this.superkingdomTaxonid = superkingdomTaxonid;
	}

	public String getKingdomTaxonid() {
		return kingdomTaxonid;
	}

	public void setKingdomTaxonid(String kingdomTaxonid) {
		this.kingdomTaxonid = kingdomTaxonid;
	}

	public String getPhylumTaxonId() {
		return phylumTaxonId;
	}

	public void setPhylumTaxonId(String phylumTaxonId) {
		this.phylumTaxonId = phylumTaxonId;
	}

	public String getTaxonclassId() {
		return taxonclassId;
	}

	public void setTaxonclassId(String taxonclassId) {
		this.taxonclassId = taxonclassId;
	}

	public String getOrderTaxonId() {
		return orderTaxonId;
	}

	public void setOrderTaxonId(String orderTaxonId) {
		this.orderTaxonId = orderTaxonId;
	}

	public String getFamilyTaxonid() {
		return familyTaxonid;
	}

	public void setFamilyTaxonid(String familyTaxonid) {
		this.familyTaxonid = familyTaxonid;
	}

	public String getGenusTaxonId() {
		return genusTaxonId;
	}

	public void setGenusTaxonId(String genusTaxonId) {
		this.genusTaxonId = genusTaxonId;
	}

	public String getSpeciesTaxonId() {
		return speciesTaxonId;
	}

	public void setSpeciesTaxonId(String speciesTaxonId) {
		this.speciesTaxonId = speciesTaxonId;
	}

	public String getFullTaxonomy( Rank resolution ){
		
		StringBuffer result = new StringBuffer();
		String[] fields = getFullTaxonomy( "\t" ).split( "\t" );

		for( int i = 0; i <= resolution.getCode(); i++ ){
			result.append( fields[i] ).append( ":" );
		}

// there are n-1 delimiters ":" in a full taxonomy. What was the resolution level here? Append any remaining ":"s.
		int remainingDelims = Rank.values().length - resolution.getCode() - 1;
		for( int i = 0; i < remainingDelims; i++ ){
			result.append(":");
		}

// we just left the ":" after the SPECIES call for ease of calculation above, but remove it here...
		return( result.substring(0, result.lastIndexOf(":")));
	}

	public String getFullTaxonomy( String delim ){
		return new StringBuffer().append( 
			this.superkingdom == null ? "" : ( this.superkingdom )).append( delim ).append(
			( this.kingdom == null ? "" :  this.kingdom )).append( delim ).append(
			( this.phylum == null ? "" :  this.phylum )).append( delim ).append(
			( this.taxonclass == null ? "" : ( this.taxonclass ))).append( delim ).append(
			( this.order == null ? "" : ( this.order ))).append( delim ).append(
			( this.family == null ? "" : ( this.family ))).append( delim ).append(
			( this.genus == null ? "" : ( this.genus ))).append( delim ).append(
			( this.species == null ? "" : ( this.species ))).toString();
	}

	public String[] getTaxonIds(){

		boolean itIsASpuh = ( this.species == null || this.species.indexOf(" sp.") != -1 );

		return new String[]{
				( this.superkingdomTaxonid == null ? "0" : this.superkingdomTaxonid ),
				( this.kingdomTaxonid == null ? "0" : this.kingdomTaxonid ),
				( this.phylumTaxonId == null ? "0" : this.phylumTaxonId ),
				( this.taxonclassId == null ? "0" : this.taxonclassId ),
				( this.orderTaxonId == null ? "0" : this.orderTaxonId ),
				( this.familyTaxonid == null ? "0" : this.familyTaxonid ),
				( this.genusTaxonId == null ? "0" : this.genusTaxonId ),
				( itIsASpuh || this.speciesTaxonId == null ? "0" : this.speciesTaxonId )
		};
	}
}

