/**
 * 
 */
package laks.mapreduce.charcount.reduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Laks.Pendyala
 *
 */
public class CharCountReducer extends Reducer<String, LongWritable, Text, LongWritable> {

	 @Override
	  public void reduce(String key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		  
		  long sum = 0;
		  
		  for(LongWritable iw:values)
		  {
			  sum += iw.get();
		  }
		  
		  context.write(new Text(key), new LongWritable(sum));
	  }
}
