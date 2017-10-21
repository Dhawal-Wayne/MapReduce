import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class miniReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		int sum=0;
		for (Text val : values) {
			sum+=Integer.parseInt(val.toString());
		}
		if(sum>0)
		context.write(_key,new Text(new Integer(sum).toString()));
		return ;
	}

}
