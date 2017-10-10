	import java.io.IOException;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Reducer;
	
	
	public class ratingsCombiner extends Reducer<Text, ratingsPairValue, Text, ratingsPairValue> {
		
		public void reduce(Text _key, Iterable<ratingsPairValue> values, Context context)
				throws IOException, InterruptedException {
			// process values
			int no_of_values=0;
			int sum_of_ratings=0;
			
			for (ratingsPairValue val : values) {
				no_of_values++;
				sum_of_ratings+=val.getPartialSum();
			}
			
			context.write(new Text(_key), new ratingsPairValue(sum_of_ratings,no_of_values));
			
			return ;
		}
		
	
	
	}
