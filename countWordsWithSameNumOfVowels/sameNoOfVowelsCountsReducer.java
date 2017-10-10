import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class sameNoOfVowelsCountsReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	public void reduce(IntWritable _key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// process values
		int count_of_words_with_same_no_of_vowels=0;
		for (IntWritable val : values) {
			
			count_of_words_with_same_no_of_vowels+=val.get();

		}
		
		context.write(_key, new IntWritable(count_of_words_with_same_no_of_vowels));
		return ;
	}

}
