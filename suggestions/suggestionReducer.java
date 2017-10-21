import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeMap;
import java.util.HashSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class suggestionReducer extends Reducer<Text, Text, Text, Text> {private TreeMap<Integer,HashSet<Integer>> suggestionsData = new TreeMap<>();void allSubGroups(int[] a, int[] used, int startIndex, int usedCount,TreeMap<Integer,HashSet<Integer>> suggestionData){if(usedCount == used.length){
HashSet<Integer> intersection  = new HashSet<>();intersection.addAll(suggestionData.get(a[used[0]]));int finalKey=a[used[0]];
for(int i = 1; i < usedCount; i++){finalKey*=10;
intersection.retainAll(suggestionData.get(a[used[i]]));
finalKey+=a[a[i]];
}if(finalKey!=0)
suggestionData.put(finalKey, intersection);
}else{for(int i = startIndex; i < a.length; i++){used[usedCount] = i;
allSubGroups(a, used, i+1, usedCount+1,suggestionData);used[usedCount] = -1;}
}
}

public void reduce(Text _key, Iterable<Text> values, Context context)
throws IOException, InterruptedException {
// process values

int noOfRecords = 0;

for (Text val : values) {

noOfRecords++;

String validsuggestion[] = val.toString().split(":")[1].split(",");
HashSet<Integer> validSuggestionSet = new HashSet<>();

for(int i=0;i<validsuggestion.length;i++)
validSuggestionSet.add(Integer.parseInt(validsuggestion[i]));

suggestionsData.put(Integer.parseInt(val.toString().split(":")[0]), validSuggestionSet);
}


Set<Integer> keySet = suggestionsData.keySet();
int keys [] = new int[noOfRecords];
int i=0;
for(Integer tempKey : keySet)
{
keys[i]=tempKey.intValue();
i++;
}

for(i=2;i<noOfRecords;i++)
{
int used[] = new int[i];
allSubGroups(keys , used , 0 , 0, suggestionsData);
}



keySet = suggestionsData.keySet();
for(Integer tempKey : keySet)
{
context.write(new Text(tempKey.toString()), new Text(suggestionsData.get(tempKey).toString()));
}
return ;
}
}

