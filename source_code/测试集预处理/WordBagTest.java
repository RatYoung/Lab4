import java.io.IOException;

import java.util.List;
import java.util.Scanner;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apdplat.word.segmentation.Word;
import org.apdplat.word.WordSegmenter;

public class WordBagTest{
	private static ArrayList<String> chi_words = new ArrayList<String>();

	public static class BagMapper extends Mapper<Object, Text, Text, IntWritable> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      		//System.out.println(value);
      		String content = value.toString().split("\t")[4];
			List<Word> words = WordSegmenter.seg(content.replaceAll("[a-zA-Z0-9]", ""));
			
			for (int i = 0; i < chi_words.size(); i++){
				Text newKey = new Text();
				IntWritable newValue = new IntWritable();
				newKey.set(chi_words.get(i));
				newValue.set(0);
				context.write(newKey, newValue);
			}

			for (int i = 0; i < words.size(); i++){
				if (chi_words.contains(words.get(i).toString())){
					Text newKey = new Text();
					IntWritable newValue = new IntWritable();
					newKey.set(words.get(i).toString());
					newValue.set(1);
					context.write(newKey, newValue);
				}
			}
		}
	}

	public static class BagCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable val : values){
				sum = sum + val.get();
			}

			IntWritable newValue = new IntWritable();
			newValue.set(sum);

			context.write(key, newValue);
		}
	}

	public static class BagReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable val : values){
				sum = sum + val.get();
			}

			IntWritable newValue = new IntWritable();
			newValue.set(sum);

			context.write(key, newValue);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		FSDataInputStream fsi = null;
		fsi = fs.open(new Path(args[0]+"/chi_words.txt"));
		Scanner scan = new Scanner(fsi);

		while (scan.hasNext()){
				String str = scan.nextLine();
				chi_words.add(str);
		}

		Job wordBagTestJob = new Job(conf, "WordBagTest");
		wordBagTestJob.setJarByClass(WordBagTest.class);
		wordBagTestJob.setMapperClass(BagMapper.class);
		wordBagTestJob.setCombinerClass(BagCombiner.class);
		wordBagTestJob.setReducerClass(BagReducer.class);
		wordBagTestJob.setMapOutputKeyClass(Text.class);
		wordBagTestJob.setMapOutputValueClass(IntWritable.class);
		wordBagTestJob.setOutputKeyClass(Text.class);
		wordBagTestJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(wordBagTestJob, new Path(args[1]));
    	FileOutputFormat.setOutputPath(wordBagTestJob, new Path(args[2]));
    	wordBagTestJob.waitForCompletion(true);
    	System.out.println("Finished");
	}
}