import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.*;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Plagiarism {
	private static int fileNumber = 1;
	private static String filePath = null;
	private static int[][] shingleMatrix = new int[500][10];
	private static int shingleCount = 0;

	private static Set<String> setA;
	private static Set<String> setB;
	private static int hashes[];
	private static double errorFactor = 0.01;
	private static int numOfHashes = (int) (1 / (errorFactor * errorFactor));
	private static int numOfSets = 2;
	private static HashMap<String, boolean[]> matrix;
	private static int[][] minHashes;
	private static int fileCount = 0;
	private static List<String> fileList = new ArrayList<String>();

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String dirName = fileSplit.getPath().toString();
			dirName = dirName.substring(0, dirName.lastIndexOf("/"));
			String filename = fileSplit.getPath().getName();

			if (filename.endsWith(".dat")) {
				HashMap<String, Integer> shingleMap = new HashMap<String, Integer>();
				String line1 = value.toString();
				StringTokenizer tokenizer = new StringTokenizer(line1);
				while (tokenizer.hasMoreElements()) {
					Configuration conf = context.getConfiguration();
					String fileName = tokenizer.nextToken();
					if (conf.get("fileName") == null) {
						conf.set("fileName", dirName + "/" + fileName);
					} else {
						conf.set("fileName", conf.get("fileName") + "|"
								+ dirName + "/" + fileName);
					}
					if (!tokenizer.hasMoreElements())
						word.set(conf.get("fileName"));
					context.write(word, one);
				}

			}

		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public Set<String> resultSet = new HashSet<String>();
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			StringTokenizer tokenizer = new StringTokenizer(key.toString(), "|");
			
			while (tokenizer.hasMoreTokens()) {
				String temp = tokenizer.nextToken();
				fileList.add(temp);
			}

			for (int i = 0; i < fileList.size(); i++) {

				Path pt1 = new Path(fileList.get(i));
				FileSystem fs1 = FileSystem.get(new Configuration());
				BufferedReader br1 = new BufferedReader(new InputStreamReader(
						fs1.open(pt1)));

				String line2 = null;
				StringTokenizer tokens2 = null;
				setA = new HashSet<String>();

				while ((line2 = br1.readLine()) != null) {
					tokens2 = new StringTokenizer(line2);
					while (tokens2.hasMoreElements()) {
						setA.add(tokens2.nextToken());
					}
				}

				for (int j = 1; j < fileList.size(); j++) {

					Path pt2 = new Path(fileList.get(j));
					FileSystem fs2 = FileSystem.get(new Configuration());
					BufferedReader br2 = new BufferedReader(
							new InputStreamReader(fs2.open(pt2)));

					String line3 = null;
					StringTokenizer tokens3 = null;
					setB = new HashSet<String>();

					while ((line3 = br2.readLine()) != null) {
						tokens3 = new StringTokenizer(line3);
						while (tokens3.hasMoreElements()) {
							setB.add(tokens3.nextToken());
						}
					}
                                                
						if (!isIdenticalHashSet(setA, setB) ||(fileList.get(i)!= fileList.get(j))){
                                                matrix = buildSetMatrix(setA, setB);
						minHashes = initMinHashes(numOfSets, numOfHashes);
						hashes = computeHashes(numOfHashes);
						double simi = findSimilarities();
						String fil1 = fileList.get(i).substring(fileList.get(i).lastIndexOf("/")+1, fileList.get(i).length());
						String fil2 = fileList.get(j).substring(fileList.get(j).lastIndexOf("/")+1, fileList.get(j).length());

						if(!fil1.equals(fil2)){

                                                if (simi >= 0.9940)
					        {
					           System.out.println("The files : " + fil1 + " & " + fil2 + " are same and the value is " + simi);
					           String temp1 = fil1 + " & " + fil2 + "---> Yes (Simillar)";
					           key = new Text(temp1);
					           if(!resultSet.contains(temp1)){
					        	   context.write(key, null);
					        	   resultSet.add(temp1);
					           }
					        }
					        else
					        {
					        	System.out.println("The files : " + fil1 + " & " + fil2 + " are not same and the value is " + simi);
					        	String temp2 = fil1 + " & " + fil2 + "---> NO  (Not Simillar)";
						           key = new Text(temp2);
						           if(!resultSet.contains(temp2)){
						        	   context.write(key, null);
						        	   resultSet.add(temp2);
						           }
					        }
					   }	
					}
				}
			}

		}

		private double findSimilarities() {
			computeMinHashForSet(setA, 0);
			computeMinHashForSet(setB, 1);
			return computeMinHash(minHashes, numOfHashes);
		}

		private void computeMinHashForSet(Set<String> set, int setIndex) {
			int hashIndex = 0;

			for (String element : matrix.keySet()) {
				for (int i = 0; i < numOfHashes; i++) {
					if (set.contains(element)) {
						int hashValue = hashes[hashIndex];
						if (hashValue < minHashes[setIndex][hashIndex]) {
							minHashes[setIndex][hashIndex] = hashValue;
						}
					}
				}
				hashIndex++;
			}
		}

		private double computeMinHash(int[][] minHashes, int numOfHashes) {
			int identicalMinHashes = 0;
			for (int i = 0; i < numOfHashes; i++) {
				if (minHashes[0][i] == minHashes[1][i]) {
					identicalMinHashes++;
				}
			}
			return (1.0 * identicalMinHashes) / numOfHashes;
		}

		private HashMap<String, boolean[]> buildSetMatrix(Set<String> setA,
				Set<String> setB) {

			HashMap<String, boolean[]> matrix = new HashMap<String, boolean[]>();

			for (String element : setA) {
				matrix.put(element, new boolean[] { true, false });
			}

			for (String element : setB) {
				if (matrix.containsKey(element)) {
					matrix.put(element, new boolean[] { true, true });
				} else if (!matrix.containsKey(element)) {
					matrix.put(element, new boolean[] { false, true });
				}
			}

			return matrix;
		}

		private int[][] initMinHashes(int numOfSets, int numOfHashes) {
			int[][] minHashes = new int[numOfSets][numOfHashes];

			for (int i = 0; i < numOfSets; i++) {
				for (int j = 0; j < numOfHashes; j++) {
					minHashes[i][j] = Integer.MAX_VALUE;
				}
			}
			return minHashes;
		}

		private int[] computeHashes(int numOfHashes) {
			int[] hashes = new int[numOfHashes];
			Random r = new Random(31);

			for (int i = 0; i < numOfHashes; i++) {
				int a = (int) r.nextInt();
				int b = (int) r.nextInt();
				int c = (int) r.nextInt();
				hashes[i] = (int) ((a * (a * b * c >> 4) + a * a * b * c + c) & 0xFFFFFFFF);
			}
			return hashes;
		}

		private boolean isIdenticalHashSet(Set<String> h1, Set<String> h2) {
			if (h1.size() != h2.size()) {
				return false;
			}
			HashSet<String> clone = new HashSet<String>(h2); 
			Iterator it = h1.iterator();
			while (it.hasNext()) {
				String A = (String) it.next();
				if (clone.contains(A)) { 
												clone.remove(A);
				} else {
					return false;
				}
			}
			return true; 
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Plagiarism");
		job.setJarByClass(Plagiarism.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Plagiarism.Map.class);
		job.setReducerClass(Plagiarism.Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(new Path(args[0]));
		for (int i = 0; i < status.length; i++) {
                String filename = status[i].getPath().toString();
		if (filename.endsWith(".dat")) {
			FileInputFormat.addInputPath(job, new Path(args[0]));
			System.out.println("&&&&&&&&&&&&&&&&&&&&&" + filename);
			filePath = args[0];
		             }
		}

         	job.waitForCompletion(true);
	}
}
