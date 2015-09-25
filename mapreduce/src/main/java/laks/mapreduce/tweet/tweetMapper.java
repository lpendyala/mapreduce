/**
 * 
 */
package laks.mapreduce.tweet;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * @author Laks.Pendyala
 *
 */
public class tweetMapper  extends Mapper <Object,Text,Text, Text> {
	
	static final String TWEETLIST  = "T^";

	@Override
	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		 
		 if (value !=null  && value.toString() !=null) {
			  
			 String[] words = value.toString().split("[,]+");
			 context.write(new Text(words[0]), new Text(TWEETLIST+value));
			
		}
	 }
}

