import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class wordsWithSameSetOfVowelsReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text _key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// process values
		int countOfWords = 0;
		for (IntWritable val : values) {
			countOfWords += val.get();
		}
		
		String finalKey = "";
		
		if(_key.toString().charAt(0)!='0')
			for(int i=0;i<_key.toString().charAt(0)-'0';i++)
			{
				finalKey+="a";
			}
		
		if(_key.toString().charAt(1)!='0')
			for(int i=0;i<_key.toString().charAt(1)-'0';i++)
			{
				finalKey+="e";			
			}
		
		if(_key.toString().charAt(2)!='0')
			for(int i=0;i<_key.toString().charAt(2)-'0';i++)
			{
				finalKey+="i";			
			}
		
		if(_key.toString().charAt(3)!='0')
			for(int i=0;i<_key.toString().charAt(3)-'0';i++)
			{
				finalKey+="o";
			}
		
		if(_key.toString().charAt(4)!='0')
			for(int i=0;i<_key.toString().charAt(4)-'0';i++)
			{
				finalKey+="u";
			}
		
		context.write(new Text(finalKey), new IntWritable(countOfWords));
		return;		
	}

}
