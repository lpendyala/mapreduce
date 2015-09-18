package laks.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import laks.mapreduce.anagram.AnagramMapper;
import laks.mapreduce.anagram.AnagramReducer;
import laks.mapreduce.charcount.CharCountMapper;
import laks.mapreduce.charcount.CharCountReducer;
import laks.mapreduce.dna.DNAMapper;
import laks.mapreduce.dna.DNAReducer;


/**
 * Hello world!
 *
 */
public class MRDriver 
{
    public static void main( String[] args ) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException
    {	if (args.length != 3) {
			System.out.printf("Usage: MRDriver <MRProgram> <input dir> <output dir>\n");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(MRDriver.class);
		job.setJobName(args[0]);
		
			
		if ("charCount".equals(args[0])) {
			
			job.setMapperClass(CharCountMapper.class);
			job.setReducerClass(CharCountReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);
			
		}else if("anagram".equals(args[0])){

			/*
			 * Find anagrams in a huge text. An anagram is basically a 
			 * different arrangement of letters in a word. Anagram does not need to be meaningful.
			 */
			
			job.setMapperClass(AnagramMapper.class);
			job.setReducerClass(AnagramReducer.class);		
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
		}else if("dna".equals(args[0])){
			
			/*
			 * A file contains the DNA sequence of people. Find all the people 
			 * who have same or mirror image of DNAs.
			 */

			job.setMapperClass(DNAMapper.class);
			job.setReducerClass(DNAReducer.class);		
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			
		}else{
			System.out.printf("Unknown MR program\n");
			System.exit(-1);
		}
		
		

		
		
		//job.setInputFormatClass(FixedLengthInputFormat.class);
		//FixedLengthInputFormat.setRecordLength(conf, 15);
		
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
    }
}
