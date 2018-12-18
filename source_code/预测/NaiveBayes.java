package lab4;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

public class NaiveBayes {
	private static ArrayList<String> pos = new ArrayList<String>();
	private static ArrayList<String> neu = new ArrayList<String>();
	private static ArrayList<String> neg = new ArrayList<String>();
	
	private static double p_pos;
	private static double p_neu;
	private static double p_neg;
	
	private static ArrayList<Double> splitVector(ArrayList<String> list) {
		ArrayList<Double> probability = new ArrayList<Double>(Collections.nCopies(2978, 0.0));
		
		for (String each : list) {
			String[] vector = each.split(" ");
			for (int i = 0; i < vector.length; i++) {
				double num = Double.parseDouble(vector[i]);
				if (num > 0) {
					probability.set(i, probability.get(i)+(1/list.size()));
				}
			}
		}
		
		return probability;
	}
	
	private static void readFile() {
		File file1 = new File("C:\\Users\\24541\\Desktop\\text_vector");
		if (file1.isFile() && file1.exists()) { 
			int count = 0;
			try {
				Scanner scanner = new Scanner(file1);
				while (scanner.hasNextLine()) {
					count = count + 1;
					String str = scanner.nextLine();
					String emotion = str.split("\t")[0].split(",")[1];
					String vector = str.split("\t")[1];
					
					if (emotion.equals("1"))
						pos.add(vector);
					else if (emotion.equals("0"))
						neu.add(vector);
					else if (emotion.equals("-1"))
						neg.add(vector);
				}
				scanner.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			
			p_pos = pos.size()/count;
			p_neu = neu.size()/count;
			p_neg = neg.size()/count;
		}	
	}
	
	public static void main(String[] args) throws Exception {
		readFile();
		
		ArrayList<Double> probabilities_pos = splitVector(pos);
		ArrayList<Double> probabilities_neu = splitVector(neu);
		ArrayList<Double> probabilities_neg = splitVector(neg);
		
		File file = new File("C:\\Users\\24541\\Desktop\\text_vector_test");
		if (file.isFile() && file.exists()) {
			String[] result = new String[56346];
			try {
				Scanner scan = new Scanner(file, "utf-8");
				int count = 0;
				System.out.println("---------------qaq-----------------");
				
				while (scan.hasNextLine()) {
					System.out.println("---------------qaq-----------------");
					String str = scan.nextLine();
					String title = str.split("\t")[0];
					String vector = str.split("\t")[1];
					String[] sub_vector = vector.split(" ");
					System.out.println(title);
					
					double p1 = p_pos;
					double p2 = p_neu;
					double p3 = p_neg;
					
					for (int i = 0; i < sub_vector.length; i++) {
						double num = Double.parseDouble(sub_vector[i]);
						if (num > 0) {
							p1 = p1 * probabilities_pos.get(i);
							p2 = p2 * probabilities_neu.get(i);
							p3 = p3 * probabilities_neg.get(i);
						}
						else {
							p1 = p1 * (1-probabilities_pos.get(i));
							p2 = p2 * (1-probabilities_neu.get(i));
							p3 = p3 * (1-probabilities_neg.get(i));
						}
					}
					
					if (p1 >= p2 && p1 >= p3)
						result[count] = title + "\t" + "positive";
					else if (p2 >= p1 && p2 >= p3)
						result[count] = title + "\t" + "neutral";
					else if (p3 >= p1 && p3 >= p2)
						result[count] = title + "\t" + "negative";
					
					count = count + 1;
				}
				scan.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			
			FileOutputStream fos=new FileOutputStream(new File("C:\\Users\\24541\\Desktop\\bayes_result.txt"));
		    OutputStreamWriter osw=new OutputStreamWriter(fos, "UTF-8");
		    BufferedWriter  bw=new BufferedWriter(osw);
		        
		    for(String each:result){
		        bw.write(each+"\t\n");
		    }
		        
		    bw.close();
		    osw.close();
		    fos.close();
		}
	}
}
