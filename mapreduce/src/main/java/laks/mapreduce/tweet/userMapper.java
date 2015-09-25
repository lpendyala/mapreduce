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
public class userMapper  extends Mapper <Object,Text,Text, Text> {
	
	static final String USERLIST  = "U^";
	 @Override
	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		 
		 if (value !=null  && value.toString() !=null) {
			  
			 String[] words = value.toString().split("[,]+");
			 context.write(new Text(words[2]), new Text(USERLIST+value));
			
		}
	 }
}

