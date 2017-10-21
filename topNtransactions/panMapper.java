import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class panMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(ivalue.toString().split(",")[0]),new Text("aadhar"+ivalue.toString().split(",")[3]));
		return ;		

	}

}
