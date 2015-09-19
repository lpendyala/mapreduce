/**
 * 
 */
package laks.mapreduce.suggestions;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Laks.Pendyala
 *
 */
public class NextWordMapper extends Mapper <Object, Text, Text,Text> {
	
	@Override
	 public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		 
		String[] words = value.toString().split("[ \t]+");
		
		for (int i = 0; i < words.length; i++) {
			
			if (i == words.length-1) {
				context.write(new Text(words[i]), new Text(""));				
			}else{
				context.write(new Text(words[i]), new Text(words[i+1]));
			}
		}
	 }

}
