package marta.taxa;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum Rank {
	// load backwards for the iterator (which works 'upwards' from spp to phylum --so far--).
	SPECIES(7), GENUS(6), FAMILY(5), ORDER(4), CLASS(3), PHYLUM(2), KINGDOM(1), SUPERKINGDOM(0);

	private static final Map<Integer,Rank> lookup = new HashMap<Integer,Rank>();

	static {
		for(Rank s : EnumSet.allOf(Rank.class)){
			lookup.put(s.getCode(), s);
		}
	}

	private int code;
	
	private Rank(int code){
	     this.code = code;
	}

	public int getCode() { return code; }

	public static Rank get( int code ){ 
		return lookup.get(code); 
	}
}