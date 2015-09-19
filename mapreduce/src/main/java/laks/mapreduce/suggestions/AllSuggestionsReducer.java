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
 * @author Laks.Pendyala Input : Key : FirstWord Value : Maps of Second words
 *         and their counts
 * 
 *         Output : Key : FirstWord Value : Maps of combined count of suggestion
 * 
 *         Happy NewYear
 * 
 */
public class AllSuggestionsReducer extends Reducer<Text, MapWritable, Text, MapWritable> {

	@Override
	public void reduce(Text key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {

		// all suggestions
		MapWritable allSuggestionsWithCounts = new MapWritable();

		// loop through mapped word counts
		for (MapWritable value : values) {

			Iterator iter = value.keySet().iterator();

			while (iter.hasNext()) {
				MapWritable suggestionMap = (MapWritable) iter.next();
				Set keys = suggestionMap.keySet();

				for (Iterator iterator = keys.iterator(); iterator.hasNext();) {
					Text suggestion = (Text) iterator.next();
					IntWritable count = (IntWritable) suggestionMap.get(suggestion);

					MapWritable grouped = (MapWritable) allSuggestionsWithCounts.get(suggestion);

					if (grouped != null) {
						IntWritable count2 = (IntWritable) grouped.get(suggestion);
						grouped.put(suggestion, new IntWritable(count.get() + count2.get()));
					} else {
						allSuggestionsWithCounts.put(suggestion, count);
					}

				}

			}
		}

		context.write(key, allSuggestionsWithCounts);

	}
}