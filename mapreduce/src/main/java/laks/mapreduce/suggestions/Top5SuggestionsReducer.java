/**
 * 
 */
package laks.mapreduce.suggestions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
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
 * Value 	: Maps of Second words and their counts
 * 
 * Output :
 * Key 		: FirstWord
 * Value 	: Maps of combined count of suggestion
 * 
 * Happy NewYear
 * 
 */
public class Top5SuggestionsReducer extends Reducer<Text, MapWritable, Text, ArrayWritable> {

	 @Override
	  public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		 
		
		for (MapWritable allSuggestionsWithCounts : values) {
			Set<Writable> frequencies = allSuggestionsWithCounts.keySet();
			List allSuggestionKeys = new ArrayList<Writable>(new TreeSet<Writable>(frequencies));
			List<IntWritable> recommededSuggestionKeys = allSuggestionKeys.subList(0, allSuggestionKeys.size() >= 5 ? 5: allSuggestionKeys.size());
			List<String> recommendedSuggestions = new ArrayList<String>();
			
			for (Writable topFrequency : recommededSuggestionKeys) {
				Text suggestion = (Text)allSuggestionsWithCounts.get(topFrequency);
				recommendedSuggestions.add(suggestion.toString());
			}

			context.write(key, new ArrayWritable((String[])recommendedSuggestions.toArray()));
		} 
	  }
}