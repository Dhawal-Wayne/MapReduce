import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class matrixMulMapperA extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		String line = ivalue.toString();
		String[] entry = line.split(",");
		String sKey = "";
		String mat = entry[0].trim();
		
		String row, col;
		
		Configuration conf = context.getConfiguration();
		
		
		try{
			
			String A_dimension1 = conf.get("A1");
			String A_dimension2 = conf.get("A2");
			String B_dimension1 = conf.get("B1");
			String B_dimension2 = conf.get("B2");
						
			int dimA1 = Integer.parseInt(A_dimension1.trim());
			int dimA2 = Integer.parseInt(A_dimension2.trim());
			int dimB1 = Integer.parseInt(B_dimension1.trim());
			int dimB2 = Integer.parseInt(B_dimension2.trim());
			
			if (dimA2 == dimB1)
			{
				if(mat.matches("a"))
				{
					for (int i =0; i < dimB2 ; i++)  
					{
						row = entry[1].trim(); 
						sKey = row+i;
						context.write(new Text(sKey),ivalue);
					}
				}	
			}
			return ;
		}
		catch(Exception exception)
		{
			//System.out.println("0505 Exception Raised "+exception);
			return ;
		}

	}

}
