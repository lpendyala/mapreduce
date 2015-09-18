/**
 * 
 */
package laks.mapreduce.anagram;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * @author Laks.Pendyala
 *
 */
public class AnagramMapper  extends Mapper <Object, Text, Text,Text> {
	
	 @Override
	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		 
		 if (value !=null  && value.toString() !=null) {
			  
			 String[] words = value.toString().split("[ \t]+");
			  for(String word:words)
			  {
				  char[] chars = word.toLowerCase().toCharArray();
				  Arrays.sort(chars);
				  String reArranged = new String(chars);
				  context.write(new Text(reArranged), new Text(reArranged));
			  }
			
		}
	 }
	 
	 public static void main(String[] args) {
		 
		 String value = " This is bad\t dog. But I like it.";
		 String[] words = value.toString().split("[ \t]+");
		  for(String word:words)
		  {
			  char[] chars = word.toLowerCase().toCharArray();
			  Arrays.sort(chars);
			  String reArranged = new String(chars);
			  System.out.println(reArranged);
		  }
	}
}

