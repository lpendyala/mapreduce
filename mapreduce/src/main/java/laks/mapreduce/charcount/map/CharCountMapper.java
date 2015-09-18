/**
 * 
 */
package laks.mapreduce.charcount.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Laks.Pendyala
 *
 */
public class CharCountMapper extends Mapper <Object, Text, Text, LongWritable> {
	
	 @Override
	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		 
		 if (value !=null  && value.toString() !=null) {
			 char [] chars =value.toString().toCharArray();
			 
			 for (int i = 0; i < chars.length; i++) {
				  context.write(new Text(String.valueOf(chars[i])), new LongWritable(1));
			}
			
		}
	 }
}
