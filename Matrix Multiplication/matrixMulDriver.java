import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class matrixMulDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new matrixMulDriver(), args);
        System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("A1", args[0]);
		conf.set("A2", args[1]);
		conf.set("B1", args[2]);
		conf.set("B2", args[3]);
		
		Job job = Job.getInstance(conf, "Matrix Mul");		
		
		job.setJarByClass(matrixMulDriver.class);
		// TODO: specify a mapper
		MultipleInputs.addInputPath(job, new Path(args[4]),TextInputFormat.class ,matrixMulMapperA.class);
   		MultipleInputs.addInputPath(job, new Path(args[5]),TextInputFormat.class, matrixMulMapperB.class);
		// TODO: specify a reducer
		job.setReducerClass(matrixMulReducer.class);
		
		
		
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(Text.class);
		
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileOutputFormat.setOutputPath(job, new Path(args[6]));

		if (!job.waitForCompletion(true))
			return -1;
		return 0;
	}

}
