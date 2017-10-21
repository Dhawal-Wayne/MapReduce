import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class suggestionsMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		if(ivalue.toString().trim()!="")
		context.write(new Text("dummy"),new Text(ivalue.toString().split("!")[0]));
		return;
	}

}
