/**
 * 
 */
package laks.mapreduce.dna;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Laks.Pendyala
 *
 */
public class DNAReducer extends Reducer<String, String, Text, Text> {

	 @Override
	  public void reduce(String key, Iterable<String> values, Context context) throws IOException, InterruptedException {
		  
		 StringBuilder dnaGroup = new StringBuilder();
		  
		  for(String value:values)
		  {
			  dnaGroup.append("\t"+value);
		  }
		  context.write(new Text(key), new Text(dnaGroup.toString()));
	  }
}
