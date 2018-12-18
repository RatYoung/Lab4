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

public class TextVectorTest{
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

			String content = value.toString().split("\t")[4];
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

      		Text newKey = new Text();
      		Text newValue = new Text();

      		newKey.set(content);
      		String str_vector = "";
      		for (double num : vector){
      			str_vector = str_vector + Double.toString(num) + " ";
      		}
      		newValue.set(str_vector);

      		context.write(newKey, newValue);
		}
	}

	public static class VectorReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String value = "";
			int count = 0;
			for (Text val : values){
				if (count >= 1)
					break;
				value = value + val.toString();
				count = count + 1;
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
		fsi = fs.open(new Path(args[0]+"/wordbag_test"));
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

		Job vectorTestJob = new Job(conf, "TextVectorTest");
		vectorTestJob.setJarByClass(TextVectorTest.class);
		vectorTestJob.setMapperClass(VectorMapper.class);
		vectorTestJob.setReducerClass(VectorReducer.class);
		vectorTestJob.setMapOutputKeyClass(Text.class);
		vectorTestJob.setMapOutputValueClass(Text.class);
		vectorTestJob.setOutputKeyClass(Text.class);
		vectorTestJob.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(vectorTestJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(vectorTestJob, new Path(args[2]));

		vectorTestJob.waitForCompletion(true);
		System.out.println("Finished");
	}
}