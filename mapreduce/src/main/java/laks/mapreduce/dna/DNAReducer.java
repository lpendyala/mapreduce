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
public class DNAReducer extends Reducer<Text, Text, Text, Text> {

	 @Override
	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		  
		 StringBuilder dnaGroup = new StringBuilder();
		  
		  for(Text value:values)
		  {
			  dnaGroup.append("\t"+value.toString());
		  }
		  context.write(key, new Text(dnaGroup.toString()));
	  }
}
