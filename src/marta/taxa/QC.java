package marta.taxa;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import java.util.Hashtable;

public class QC {

	public static void main( String[] args ){
		resplit(args[0]);

	}

	public static boolean resplit(String seqfile) {

		BufferedWriter writer = null;
		BufferedReader reader;
		int readIndex = 0; 

		try{

			reader = new BufferedReader( new InputStreamReader( new FileInputStream( seqfile ), "UTF8"));

			Hashtable<String,String[]> seqs = new Hashtable<String,String[]>();
			//Vector<String[]> lines = new Vector<String[]>();
			String line;
			String[] read = null, fields;

//
// read the lines from the original fasta file.
			while((line = reader.readLine()) != null ){

				if( line.startsWith(">")){

					//the fields are (declare these final ints)
					//1. Id
					//2. Name
					//3. Sequence
					read = new String[3];
					fields = line.split(";");
					fields = fields[0].split(" ");
					read[0] = fields[0].substring(1); // clip off the fasta prefix '>'
					read[1] = new StringBuffer( fields[1] ).append( " " ).append( fields[2] ).toString();
					read[2] = "";
					seqs.put( read[0], read );

				} else { 

					read = seqs.get( read[0] ); // operates off of the last key. can't think of other place to put this; need to store it at '>', which means when the last cr comes before the new '>' I won't know.
					read[2] = read[2].concat(line);
				}
			}

// write the reads to file after analysis.
// or for now, loop through the first four lines to see how they look.
			writer = new BufferedWriter(
					new OutputStreamWriter(
							new FileOutputStream(
									new StringBuffer().append(
									seqfile.replace(seqfile.substring(
											seqfile.lastIndexOf(".")),".tsv")).toString(), false), "UTF-8"));

			writer.write(new StringBuffer("id\tTaxa\tSequence\n").toString());

			StringBuffer buffer = new StringBuffer();

			java.util.Enumeration<String> e = seqs.keys();
			while( e.hasMoreElements()){
				String key = (String)e.nextElement();
				String[] set = (String[])seqs.get( key );

				for( int i = 0; i < set.length; i++ ){
					String fld = set[i];
					buffer.append(fld).append((i == (set.length - 1) ? "" : '\t'));
				}

				buffer.append('\n');
			}

			writer.write(buffer.toString());
			writer.flush();
			writer.close();
			return true;

		} catch( IOException ex ){
			ex.printStackTrace();

		} catch( Exception ex ){
			ex.printStackTrace();
			System.err.println("At line: "+ readIndex);

		} finally {}

		return false;
	}
}
