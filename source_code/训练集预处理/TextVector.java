import java.util.Scanner;
import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apdplat.word.segmentation.Word;
import org.apdplat.word.WordSegmenter;

public class TextVector{
	private static ArrayList<String> chi_words = new ArrayList<String>();
	private static ArrayList<Integer> frequency = new ArrayList<Integer>();
	private static int sum;

	public static class VectorMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			ArrayList<Double> vector = new ArrayList<Double>();
			for (int i = 0; i < frequency.size(); i++){
				double zero = 0;
				vector.add(zero);
			}

			String content = value.toString();
			List<Word> words = WordSegmenter.seg(content.replaceAll("[a-zA-Z0-9]", ""));
			for (Word word : words){
				int count = 0;
				ArrayList<String> sub_words = new ArrayList<String>();

				if(chi_words.contains(word.toString())){
					sub_words.add(word.toString());
					for (int i = 0; i < words.size(); i++){
						if (word.toString().equals(words.get(i).toString())) {
							count = count + 1;
						}
					}

					double tf = count/sub_words.size();
					double idf = Math.log(sum/frequency.get(chi_words.indexOf(word.toString())));
					double tf_idf = tf * idf;

					vector.set(chi_words.indexOf(word.toString()), tf_idf);
				}
			}

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
      		String parentFileName = fileSplit.getPath().getParent().getName();
      		String fileName = fileSplit.getPath().getName();

      		Text newKey = new Text();
      		Text newValue = new Text();
      		String str_vector = "";
      		for (double num : vector){
      			str_vector = str_vector + Double.toString(num) + " ";
      		}
      		newValue.set(str_vector);

      		if (parentFileName.contains("positive")){
      			newKey.set(fileName+","+"1");
      		}
      		else if (parentFileName.contains("neutral")){
      			newKey.set(fileName+","+"0");
      		}
      		else if (parentFileName.contains("negative")){
      			newKey.set(fileName+","+"-1");
      		}

      		context.write(newKey, newValue);
		}
	}

	public static class VectorReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String value = "";
			for (Text val : values){
				value = value + val.toString();
			}
			Text newValue = new Text();
			newValue.set(value);
			context.write(key, newValue);
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		FSDataInputStream fsi = null;
		fsi = fs.open(new Path(args[0]+"/word_bag"));
		Scanner scan = new Scanner(fsi);

		while (scan.hasNext()){
			String str = scan.nextLine();
			String[] content = str.split("\t");
			chi_words.add(content[0]);
			frequency.add(Integer.parseInt(content[1]));
		}

		sum = 0;
		for (int num : frequency){
			sum = sum + num;
		}

		Job vectorJob = new Job(conf, "TextVector");
		vectorJob.setJarByClass(TextVector.class);
		vectorJob.setMapperClass(VectorMapper.class);
		vectorJob.setReducerClass(VectorReducer.class);
		vectorJob.setMapOutputKeyClass(Text.class);
		vectorJob.setMapOutputValueClass(Text.class);
		vectorJob.setOutputKeyClass(Text.class);
		vectorJob.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(vectorJob, new Path(args[1]), new Path(args[2]), new Path(args[3]));
		FileOutputFormat.setOutputPath(vectorJob, new Path(args[4]));

		vectorJob.waitForCompletion(true);
		System.out.println("Finished");

		System.out.println(chi_words.size());
		System.out.println(frequency.size());
		System.out.println(sum);
	}
}