/**
 * 
 */
package laks.mapreduce.suggestions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Laks.Pendyala
 * Input 	:
 * Key   	: FirstWord
 * Value 	: Maps of suggestions and their counts
 * 
 * Output :
 * Key 		: FirstWord
 * Value 	: Array of top 5 suggestions
 */
public class SuggestionsReducer extends Reducer<Text, MapWritable, Text, ArrayWritable> {

	 @Override
	  public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		 
		 MapWritable allSuggestionsWithCounts = new MapWritable();
		 
		 for (MapWritable value : values) {
			 allSuggestionsWithCounts.putAll(value);
		 }
		 
		Set<Writable> frequency = allSuggestionsWithCounts.keySet();
		List allSuggestionKeys = new ArrayList<Writable>(new TreeSet<Writable>(frequency));
		List<IntWritable> recommededSuggestionKeys = allSuggestionKeys.subList(0, allSuggestionKeys.size() >= 5 ? 5: allSuggestionKeys.size());
		
		String[] recommendedSuggestions = new String[recommededSuggestionKeys.size()];
		
		for (Writable topFrequency : recommededSuggestionKeys) {
			//Text suggestion = allSuggestionsWithCounts.get(topFrequency);
			
		}
		
		 
		
	  }
}