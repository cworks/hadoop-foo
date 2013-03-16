/**	
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Class: Main
 * File: Main.java
 * Created: 2013-03-05 19:57:52.376010
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 */
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: MapReduce job that will produce a list containing all MLB players
 * and their career AB stat.
 * @author corbett
 */
public class MostAtBats {

	/**
	 * main method
	 * @param args
	 */
    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(MostAtBats.class);
        logger.info("MostAtBats starting up...");

	    /*
	     * The expected command-line arguments are the paths containing
	     * input and output data. Terminate the job if the number of
	     * command-line arguments is not exactly 2.
	     */
	    if (args.length != 2) {
	      System.out.println("Usage: MostAtBats <Batting.csv> <MostAtBats.csv>\n");
	      System.exit(-1);
	    }

	    /*
	     * Instantiate a Job object for your job's configuration.  
	     */
	    Job job = new Job();
	    
	    /*
	     * Specify the jar file that contains your driver (MostAtBats class), mapper, and reducer.
	     * Hadoop will transfer this jar file to nodes in your cluster running mapper and reducer
	     * tasks.
	     */
	    job.setJarByClass(MostAtBats.class);
	    
	    /*
	     * Specify an easily-decipherable name for the job.
	     * This job name will appear in reports and logs.
	     */
	    job.setJobName("Most at Bats");

	    /*
	     * Specify the paths to the input and output data based on the
	     * command-line arguments.
	     */
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    /*
	     * Specify the mapper and reducer classes.
	     */
	    job.setMapperClass(AtBatMapper.class);
	    job.setReducerClass(AtBatSumReducer.class);

	    /*
	     * Specify the job's output key and value classes (output from reducer)
	     */
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    /*
	     * Start the MapReduce job and wait for it to finish.
	     * If it finishes successfully, return 0. If not, return 1.
	     */
	    boolean success = job.waitForCompletion(true);
	    System.exit(success ? 0 : 1);
    }
}
