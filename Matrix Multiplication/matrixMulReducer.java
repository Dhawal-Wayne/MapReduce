import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class matrixMulReducer extends Reducer<Text, Text, Text, IntWritable> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		Configuration conf = context.getConfiguration();
		
		String A_dimension2 = conf.get("A2");
		String B_dimension1 = conf.get("B1");
		
		int dimA2 = Integer.parseInt(A_dimension2);
		int dimB2 = Integer.parseInt(B_dimension1);
		
		int[] row = new int[dimA2]; 
		int[] col = new int[dimB2];
		
		for(Text val : values)
		{
			String[] entries = val.toString().split(",");
			if(entries[0].matches("a"))
			{
				int index = Integer.parseInt(entries[2].trim());
				row[index] = Integer.parseInt(entries[3].trim());
			}
			if(entries[0].matches("b"))
			{
				int index = Integer.parseInt(entries[1].trim());
				col[index] = Integer.parseInt(entries[3].trim());
			}
		}
		
		int total = 0;
		for(int i = 0 ; i < dimA2 ; i++)
		{
			total += row[i]*col[i];
		}
		
		context.write(key, new IntWritable(total));
		
		return ;
	}

}
