/**
 * 
 */
package laks.mapreduce.suggestions;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Laks.Pendyala
 * Input :
 * Key : First word
 * Values : List of  second words
 * 
 * Output :
 * Key : First word
 * Values : Map of second words and count
 *
 *
 */
public class NextWordReducer extends Reducer<Text, Text, Text, MapWritable> {

	 @Override
	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
		 MapWritable counter = new MapWritable();
		 
		 for (Text value : values) {
			IntWritable count = (IntWritable)counter.get(key);
			
			if (count == null) {
				counter.put(value, new IntWritable(1));
			}else{
				count.set(count.get()+1);
				counter.put(value, count);
			}
		 }
		 context.write(key, counter);
	  }
}
