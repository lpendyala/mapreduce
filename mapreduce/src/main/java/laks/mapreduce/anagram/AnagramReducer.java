/**
 * 
 */
package laks.mapreduce.anagram;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Laks.Pendyala
 *
 */
public class AnagramReducer extends Reducer<String, String, Text, Text> {

	 @Override
	  public void reduce(String key, Iterable<String> values, Context context) throws IOException, InterruptedException {
		  
		 StringBuilder anagrams = new StringBuilder();
		  
		  for(String value:values)
		  {
			  anagrams.append(value);
		  }
		  context.write(new Text(key), new Text(anagrams.toString()));
	  }
}
