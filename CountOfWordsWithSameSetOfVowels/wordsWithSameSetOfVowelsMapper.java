import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class wordsWithSameSetOfVowelsMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		String words[] = ivalue.toString().split(" ");
		
		
		for(String word : words)
		{
			TreeMap<String,Integer> keyGENMap = new TreeMap<>();
			
			keyGENMap.put("a", 0);
			keyGENMap.put("e", 0);
			keyGENMap.put("i", 0);
			keyGENMap.put("o", 0);
			keyGENMap.put("u", 0);

			for(int i=0;i<word.length();i++)
			{
				if(word.charAt(i)=='a')
					keyGENMap.put("a", keyGENMap.get("a")+1);
				
				else if(word.charAt(i)=='e')
					keyGENMap.put("e", keyGENMap.get("e")+1);
				
				else if(word.charAt(i)=='i')
					keyGENMap.put("i", keyGENMap.get("i")+1);
				
				else if(word.charAt(i)=='o')
					keyGENMap.put("o", keyGENMap.get("o")+1);
				
				else if(word.charAt(i)=='u')
					keyGENMap.put("u", keyGENMap.get("u")+1);
			}

			Set<String> keySet = keyGENMap.keySet();
			
			String finalKey = "";
			for(String tempKey:keySet)
			{
				finalKey+=keyGENMap.get(tempKey).toString();
			}
			if(finalKey!="")
			context.write(new Text(finalKey), new IntWritable(1));
		}
		
		
		return ;
	}

}
