import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class transactionsReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		if(_key.toString().charAt(0)=='p')
		{
			int sum=0;
			String key="";
			for(Text val : values)
			{
				if(val.toString().charAt(0)=='a')
					key+=val.toString().split("r")[1];
				else
					sum+=Integer.parseInt(val.toString());
			}
			context.write(new Text(key),new Text(new Integer(sum).toString()));
		}
		else
		{
			int sum=0;
			for (Text val : values) 
			{
				sum+=Integer.parseInt(val.toString());				
			}			
			context.write(_key,new Text(new Integer(sum).toString()));
		}
		return;
	}

}
