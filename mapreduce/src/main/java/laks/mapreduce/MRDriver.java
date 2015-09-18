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
import laks.mapreduce.charcount.CharCountMapper;
import laks.mapreduce.charcount.CharCountReducer;


/**
 * Hello world!
 *
 */
public class MRDriver 
{
    public static void main( String[] args ) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException
    {	if (args.length != 3) {
			System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		Job job = new Job(conf, "charCount");
		job.setJarByClass(MRDriver.class);
		
		Class mapperClass =null;
		Class reducerClass =null ;
		
		if ("charcount".equals(args[0])) {
			
			mapperClass = CharCountMapper.class;
			reducerClass = CharCountReducer.class;
		}else if("anagram".equals(args[0])){
			mapperClass = AnagramMapper.class;
			reducerClass = CharCountReducer.class;
		}else{
			System.out.printf("Unknown MR program\n");
			System.exit(-1);
		}
		
		
		job.setMapperClass(mapperClass);
		job.setReducerClass(reducerClass);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//job.setInputFormatClass(FixedLengthInputFormat.class);
		//FixedLengthInputFormat.setRecordLength(conf, 15);
		
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
    }
}
