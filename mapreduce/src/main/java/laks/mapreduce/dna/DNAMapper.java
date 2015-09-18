/**
 * 
 */
package laks.mapreduce.dna;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * @author Laks.Pendyala
 *
 */
public class DNAMapper  extends Mapper <Object, Text, Text,Text> {
	
	 @Override
	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		 
		 if (value !=null  && value.toString() !=null) {
			  
			 String dNASeq1 = value.toString();
			 String dNASeq2 = (new StringBuilder(dNASeq1)).reverse().toString();
			 context.write(new Text(dNASeq1+"-"+dNASeq2), new Text(dNASeq1));
			
		}
	 }
	 
	 public static void main(String[] args) {
		 
	}
}

