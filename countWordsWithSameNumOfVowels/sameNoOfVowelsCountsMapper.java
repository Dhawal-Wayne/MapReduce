import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class sameNoOfVowelsCountsMapper extends
		Mapper<LongWritable, Text, IntWritable, IntWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		String words[] = ivalue.toString().split(" ");
		
		for(String word : words)
		{
			int vowelCount=0;
			word=word.toLowerCase();
			for(int i=0;i<word.length();i++)
			{
				if(word.charAt(i)=='a'||
						word.charAt(i)=='e'||
						word.charAt(i)=='i'||
						word.charAt(i)=='o'||
						word.charAt(i)=='u')
				{
					vowelCount++;
				}
			}
			if(vowelCount>0)
			context.write(new IntWritable(vowelCount),new IntWritable(1));
		}
		
		return;

	}

}
