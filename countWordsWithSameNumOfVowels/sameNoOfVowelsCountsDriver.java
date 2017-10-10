import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class sameNoOfVowelsCountsDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Numbers With Same No Of Vowel Count");
		job.setJarByClass(sameNoOfVowelsCountsDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(sameNoOfVowelsCountsMapper.class);
		// TODO: specify a combiner
		job.setCombinerClass(sameNoOfVowelsCountsReducer.class);
		// TODO: specify a reducer
		job.setReducerClass(sameNoOfVowelsCountsReducer.class);

		// TODO: specify Mapper output types
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// TODO: specify output types
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if (!job.waitForCompletion(true))
			return;
	}

}
