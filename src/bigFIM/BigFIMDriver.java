package bigFIM;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BigFIMDriver extends Configured implements Tool {

	public static final String NUMBER_OF_LINES_KEY = "number_of_lines_read";
	public static final String SUBDB_SIZE = "sub_db_size";
	
	private static final String OFis = "fis";
	public static final String rExt = "-r-00000";

	private static ArrayList<Double> eachLevelRunningTime = new ArrayList<Double>();
	private static ArrayList<Long> eachLeveltidsetItemsetsNum = new ArrayList<Long>();  //count the number of each frequentPattern   20170416
	private static int count = 0; //count的目的就是为了统计运行了几次MR任务，为记录时间次数做统计
	
	private String[] moreParas;
	private static String dataBaseName;
	private static double relativeMinSupport;
	private String input_file;
	private String output_dir;
	private static int dataSize;
	private static int numMappers;
	private static int childJavaOpts;
	private static long minsup;
	private static int prefix_length;
	
	//Whether to write the actually write the results. If set to false, only the timings will be reported. Default is true.
		private static boolean write_sets = false;

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.setLong("minsup", minsup);
		conf.setInt("numMappers", numMappers);
		conf.setInt("prefix_length", prefix_length);
		conf.set("mapreduce.task.timeout", "6000000");
		conf.set("mapreduce.map.java.opts", "-Xmx"+childJavaOpts+"M");
		conf.set("mapreduce.reduce.java.opts", "-Xmx"+(childJavaOpts*2)+"M");
		
		for( int k = 0; k < moreParas.length && moreParas.length >= 2; k+=2) {
			conf.set( moreParas[ k ], moreParas[ k+1 ] );			
		}
		
		try{
		    String inputDir = input_file;
		    String outputDir = output_dir;
		    Tools.cleanDirs(new String[] {outputDir});
		    long start = System.currentTimeMillis();
		    long nrLines = startAprioriPhase(inputDir, outputDir, conf);
		    startCreatePrefixGroups(inputDir, outputDir, conf, nrLines);
		    startMining(outputDir, conf);
		    long end = System.currentTimeMillis();
		    
		    
		    saveResult((end - start) / 1000.0);
		    } catch(Exception e){
		    	e.printStackTrace();
		    	File resultFile = new File("BigFIM_" + dataBaseName + "_ResultOut");
				BufferedWriter br  = new BufferedWriter(new FileWriter(resultFile, true));  // true means appending content to the file //here create a non-existed file
				br.write("BigFIM Exception occurs at minimumSupport(relative) "  + relativeMinSupport);
				br.write("\n");
				br.flush();
				br.close();
		    }
		
		return 0;
	}

	private static long startAprioriPhase(String inputFile, String outputFile, Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		long nrLines = -1;
		int prefixSize = prefix_length;
		for (int i = 1; i <= prefixSize; i++) {
			String outputDir = outputFile + "/" + "ap" + i;
			String cacheFile = outputFile + "/" + "ap" + (i - 1) + "/" + "part-r-00000";
			System.out.println("[AprioriPhase]: Phase: " + i + " input: " + inputFile + ", output: " + outputFile);

			if (nrLines != -1) {
				conf.setLong(NUMBER_OF_LINES_KEY, nrLines);
			}

			Job job = Job.getInstance(conf, "Apriori Phase" + i);
			job.setJarByClass(BigFIMDriver.class);

			if (i > 1) {
				job.addCacheFile(URI.create(cacheFile));
			}
			
			FileInputFormat.addInputPath(job, new Path(inputFile));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			job.setMapperClass(AprioriPhaseMapper.class);
			job.setReducerClass(AprioriPhaseReducer.class);

			job.setInputFormatClass(SplitByNumberOfMappersTextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setNumReduceTasks(1);

			long start = System.currentTimeMillis();
			job.waitForCompletion(true);
			long end = System.currentTimeMillis();
			count++; //20170227  -jiang  
			System.out.println("Job Apriori Phase " + i + " took " + (end - start) / 1000.0 + "s");
			eachLevelRunningTime.add((end - start) / 1000.0);
			eachLeveltidsetItemsetsNum.add(job.getCounters().findCounter(Counter.FrePattern).getValue());

			if (i == 1) {
				nrLines = job.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
			}
		}
		return nrLines;
	}

	private static void startCreatePrefixGroups(String inputFile, String outputDir, Configuration conf, long nrLines)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		/*
		 * String cacheFile = outputDir + separator + "ap" +
		 * config.getPrefixLength() + separator + "part-r-00000"; String
		 * outputFile = outputDir + separator + "pg";
		 */
		String cacheFile = outputDir + "/" + "ap" + prefix_length + "/" + "part-r-00000";
		String outputFile = outputDir + "/" + "pg";
		System.out.println("[CreatePrefixGroups]: input: " + inputFile + ", output: " + outputDir);

		
		int subDbSize = (int) Math.ceil(1.0 * nrLines / numMappers);
		conf.setLong(NUMBER_OF_LINES_KEY, nrLines);
		conf.setInt(SUBDB_SIZE, subDbSize);

		Job job = Job.getInstance(conf, "Create Prefix Groups");
		job.setJarByClass(BigFIMDriver.class);

		job.addCacheFile(URI.create(cacheFile));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntArrayWritable.class);
		job.setOutputKeyClass(IntArrayWritable.class);
		job.setOutputValueClass(IntArrayWritable.class);

		job.setMapperClass(ComputeTidListMapper.class);
		job.setReducerClass(ComputeTidListReducer.class);

		job.setInputFormatClass(SplitByNumberOfMappersTextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(inputFile));
		FileOutputFormat.setOutputPath(job, new Path(outputFile));

		

		long start = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		count++;
		System.out.println("Job Prefix Creation took " + (end - start) / 1000.0 + "s");
		eachLevelRunningTime.add((end - start) / 1000.0);
	}

	private static void startMining(String outputDir, Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {

		String inputFilesDir = outputDir + "/" + "pg" + "/";
		String outputFile = outputDir + "/" + OFis;
		System.out.println("[StartMining]: input: " + inputFilesDir + ", output: " + outputFile);

		Job job = Job.getInstance(conf, "Start Mining");
		job.setJarByClass(BigFIMDriver.class);

		job.setOutputKeyClass(Text.class);

		if (write_sets) {
			job.setOutputValueClass(Text.class);
			job.setMapperClass(EclatMinerMapper.class);
			job.setReducerClass(EclatMinerReducer.class);
		} else {
			job.setOutputValueClass(LongWritable.class);
			job.setMapperClass(EclatMinerMapperSetCount.class);
			job.setReducerClass(EclatMinerReducerSetCount.class);
		}

		job.setInputFormatClass(NoSplitSequenceFileInputFormat.class);
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		

		

		List<Path> inputPaths = new ArrayList<Path>();

		FileSystem fs = FileSystem.get(conf);
		FileStatus[] listStatus = fs.globStatus(new Path(inputFilesDir + "bucket*"));
		fs.close();
		for (FileStatus fstat : listStatus) {
			inputPaths.add(fstat.getPath());
		}

		FileInputFormat.setInputPaths(job, inputPaths.toArray(new Path[inputPaths.size()]));
		FileOutputFormat.setOutputPath(job, new Path(outputFile));
		
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		
		long start = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		count++;
		System.out.println("Job Mining took " + (end - start) / 1000.0 + "s");
		eachLevelRunningTime.add((end - start) / 1000.0);
		eachLeveltidsetItemsetsNum.add(job.getCounters().findCounter(Counter.FrePattern).getValue());
	}

	public static void saveResult(double totalTime){
		try{
			BufferedWriter br = null;
			double TotalJobRunningTime = 0;
			long TotalFrePattern = 0;
			for(int i=0; i<count; i++){
				TotalJobRunningTime += eachLevelRunningTime.get(i);
			}
			
			for(int i=0; i<eachLeveltidsetItemsetsNum.size(); i++)
				TotalFrePattern +=  eachLeveltidsetItemsetsNum.get(i);
			
			File resultFile = new File("BigFIM_" + dataBaseName + "_ResultOut");
			if(!resultFile.exists()){
				br  = new BufferedWriter(new FileWriter(resultFile, true));
				br.write("algorithmName" + "\t" + "datasetName" + "\t" + "DBSize" + "\t" + "minSuppPercentage(relative)" + "\t" + "minSupp(absolute)" +  "\t" + "childJavaOpts" + "\t" + "numMappers" + "\t" + "prefixLength" + "\t" + "TotalFrequentPattern" + "\t" + "TotalTime" + "\t" + "TotalJobTime"  + "\t");
				
				for( int i = 0; i<eachLeveltidsetItemsetsNum.size(); i++)  {
					br.write("Level_" + (i+1) + "_ItemsetsNum" + "\t");
				}
				
				for( int i = 0; i<count; i++)  {
					br.write("Level_" + (i+1) + "_JobRunningTime" + "\t");
				}
						
				br.write("\n");
			}else{
				br  = new BufferedWriter(new FileWriter(resultFile, true));
			}
			
			br.write("BigFIM" + "\t" + dataBaseName + "\t" + dataSize + "\t" + relativeMinSupport * 100.0 + "\t" + minsup + "\t" + childJavaOpts + "\t" + numMappers + "\t" + prefix_length + "\t" + TotalFrePattern + "\t" + totalTime + "\t" +TotalJobRunningTime + "\t");
			
			for( int i = 0; i<eachLeveltidsetItemsetsNum.size(); i++)  {
				br.write(eachLeveltidsetItemsetsNum.get(i) + "\t");
			}
			for(int i=0; i<count; i++) {	
				br.write(eachLevelRunningTime.get(i) + "\t"); 	
			}
			
			br.write("\n");
			br.flush();
			br.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public BigFIMDriver(String[] args) {
		int numFixedParas = 8; // datasetName, minsup(relative), inpu_file, output_file, 
		                       // datasize, childJavaOpts, nummappers, prefix_length.
		int numMoreParas = args.length - numFixedParas;
		if (args.length < numFixedParas || numMoreParas % 2 != 0) {
			System.out.println("The Number of the input parameters is Wrong!!");
			System.exit(1);
		} else {
			if (numMoreParas > 0) {
				moreParas = new String[numMoreParas];
				for (int k = 0; k < numMoreParas; k++) {
					moreParas[k] = args[numFixedParas + k];
				}
			} else {
				moreParas = new String[1];
			}
		}
		dataBaseName = args[0];
		relativeMinSupport = Double.parseDouble(args[1]);
		input_file = args[2];
		output_dir = args[3];
		dataSize = Integer.parseInt(args[4]);
		minsup = (long)Math.ceil(relativeMinSupport * dataSize);
		childJavaOpts = Integer.parseInt(args[5]);
		numMappers = Integer.parseInt(args[6]);
		prefix_length = Integer.parseInt(args[7]);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new BigFIMDriver(args), args);
		System.out.println(res);
	}
}
