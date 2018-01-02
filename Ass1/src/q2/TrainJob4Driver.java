package q2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TrainJob4Driver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    Job job = Job.getInstance(getConf()) ;
   	job.setJarByClass(getClass());
   	job.setJobName("Avg Speed of Train Each Direction Each Hour ");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(TrainJob4Mapper.class);
    job.setReducerClass(TrainJob4Reducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    
    int returnValue = job.waitForCompletion(true) ? 0:1;
	System.out.println("job.isSuccessful= " + job.isSuccessful()+"\nReudce Task= "+	job.getNumReduceTasks());
	return returnValue;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new TrainJob4Driver(), args);
    System.exit(exitCode);
  }
}
