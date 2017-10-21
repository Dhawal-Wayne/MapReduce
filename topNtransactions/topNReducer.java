import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class topNReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		int no_of_records=0;
		TreeMap<Integer,Integer> treeMap = new TreeMap<>();
		for (Text val : values) {
			String words[] = val.toString().split("\t");
			int keyValPair[] = {Integer.parseInt(words[0]),Integer.parseInt(words[1])};
			treeMap.put(keyValPair[0],keyValPair[1]);
			no_of_records++;
		}
		Set<Integer> keySet = treeMap.keySet();
		TreeMap<Integer,ArrayList<Integer>> amountSorted = new TreeMap<>();
		for(Integer key : keySet)
		{
			if(!amountSorted.containsKey(treeMap.get(key)))
			{
				amountSorted.put(treeMap.get(key),new ArrayList<Integer>());
				amountSorted.get(treeMap.get(key)).add(key);
			}
			else
				amountSorted.get(treeMap.get(key)).add(key);
		}
		int counter = 0;
		Set<Integer> sortedSet = amountSorted.keySet();
		for(Integer sKey : sortedSet)
		{
			for(Integer i : amountSorted.get(sKey))
			{
				if(no_of_records-counter<=5)
					context.write(new Text(i.toString()), new Text(sKey.toString()));
				counter++;
			}
		}
		return ;
	}

}
