import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.lang.Math;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class NearestNeighbor {
	private static ArrayList<String> emotions = new ArrayList<String>();
	private static ArrayList<String> vectors = new ArrayList<String>();

	public static class NearestMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//double score = -1;
			//int emotion = -2;

			String vector = value.toString().split("\t")[1];
			String[] own_vector = vector.split(" ");

			ArrayList<Double> kneighbor = new ArrayList<Double>(Collections.nCopies(15, 0.0));
			ArrayList<Integer> emotion = new ArrayList<Integer>(Collections.nCopies(15, -2));

			for (String each : vectors) {
				String[] target_vector = each.split(" ");
				double sub_score = 0;

				if (own_vector.length != target_vector.length) {
					System.out.println("............................................");
					System.out.println(own_vector.length);
					System.out.println(target_vector.length);
					System.out.println(value.toString().split("\t")[0]);
				}
				for (int i = 0; i < own_vector.length; i++){
					double own = Double.parseDouble(own_vector[i]);
					double target = Double.parseDouble(target_vector[i]);

					sub_score = sub_score + Math.pow((own - target), 2);
				}

				sub_score = Math.sqrt(sub_score);
				for (int i = 0; i < kneighbor.size(); i++) {
					if (kneighbor.get(i) > sub_score) {
						kneighbor.set(i, sub_score);
						emotion.set(i, Integer.parseInt(emotions.get(vectors.indexOf(each))));
					}
				}
			}

			Text newKey = new Text();
			Text newValue = new Text();
			newKey.set(value.toString().split("\t")[0]);

			int count0 = 0;
			int count1 = 0;
			int count2 = 0;

			for (int each : emotion) {
				if (each == 1)
					count0 = count0 + 1;
				else if (each == 0)
					count1 = count1 + 1;
				else if (each == -1)
					count2 = count2 + 1;
			}

			if (count2 >= count1 && count2 >= count0)
				newValue.set("negative");
			else if (count1 >= count0 && count1 >= count2)
				newValue.set("neutral");
			else if (count0 >= count1 && count0 >= count2)
				newValue.set("positive");

			context.write(newKey, newValue);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job nearestJob = new Job(conf, "NearestNeighbor");
		FileSystem fs = FileSystem.get(conf);

		FSDataInputStream fsi = null;
		fsi = fs.open(new Path(args[0]+"/text_vector"));
		Scanner scan = new Scanner(fsi);

		while (scan.hasNext()){
				String str = scan.nextLine();
				emotions.add(str.split("\t")[0].split(",")[1]);
				vectors.add(str.split("\t")[1]);
		}

		nearestJob.setJarByClass(NearestNeighbor.class);
		nearestJob.setMapperClass(NearestMapper.class);
		//nearestJob.setReducerClass(NearestReducer.class);
		nearestJob.setMapOutputKeyClass(Text.class);
		nearestJob.setMapOutputValueClass(Text.class);
		nearestJob.setOutputKeyClass(Text.class);
		nearestJob.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(nearestJob, new Path(args[1]));
    	FileOutputFormat.setOutputPath(nearestJob, new Path(args[2]));
    	nearestJob.waitForCompletion(true);
    	System.out.println("Finished");
	}
}