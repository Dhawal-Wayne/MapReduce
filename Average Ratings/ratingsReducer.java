import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ratingsReducer extends Reducer<Text, ratingsPairValue, Text, DoubleWritable> {

	public void reduce(Text _key, Iterable<ratingsPairValue> values, Context context)
			throws IOException, InterruptedException {
		// process values
		Long no_of_values=0L;
		Long sum_of_ratings=0L;
		
		for (ratingsPairValue val : values) {
			no_of_values+=val.getPartialCount();
			sum_of_ratings+=val.getPartialSum();
		}
		
		context.write(new Text(_key), new DoubleWritable(sum_of_ratings/(double)no_of_values));
		
		return ;
	}

}
