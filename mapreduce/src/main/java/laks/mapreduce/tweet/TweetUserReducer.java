/**
 * 
 */
package laks.mapreduce.tweet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Laks.Pendyala
 *
 */
public class TweetUserReducer extends Reducer<String, Text, Text, Text> {

	static final String USERLIST  = "U^";
	static final String TWEETLIST  = "T^";

	
	@Override
	  public void reduce(String key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		  
		String userDetails = new String();
		
		List<String> tweetList = new ArrayList<String>();
	
		 
		for(Text value:values){

			if (value.toString().startsWith(TWEETLIST)) {
				 tweetList.add(value.toString().substring(2, value.toString().length()));
			 }
			 
			 if (value.toString().startsWith(USERLIST)) {
				 userDetails = value.toString().substring(2, value.toString().length());
			 }
			 
		}
		  
		for (String tweetDetails : tweetList) {
			context.write(new Text(key), new Text(userDetails+","+tweetDetails));
	
		}  
		  
	  }
}
