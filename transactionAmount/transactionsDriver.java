import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class transactionsDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Sum Calculation JOB1");
		job.setJarByClass(transactionsDriver.class);
		// TODO: specify a mapper
		MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,panMapper.class);
		MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,transactionsMapper.class);
		  
		// TODO: specify a reducer
		job.setReducerClass(transactionsReducer.class);
	
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileOutputFormat.setOutputPath(job, new Path("tempout"));
		
		boolean success = job.waitForCompletion(true);
		if (success)
		{
			Job job2 = Job.getInstance(conf, "Sum Calculation JOB2");
			
			// TODO: specify a mapper
			job2.setMapperClass(miniMapper.class);		  
			// TODO: specify a reducer
			job2.setReducerClass(miniReducer.class);
			
			
			// TODO: specify output types
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			// TODO: specify input and output DIRECTORIES (not files)
			FileInputFormat.setInputPaths(job2, new Path("tempout"));
			FileOutputFormat.setOutputPath(job2, new Path(args[2]));
			
			success = job2.waitForCompletion(true);
		}
		return;
	}

}
