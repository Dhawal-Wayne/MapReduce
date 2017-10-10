import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ratingsMapper extends Mapper<LongWritable, Text, Text, ratingsPairValue> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {

		String words[] = ivalue.toString().split(",");


		context.write(new Text(words[0]), new ratingsPairValue(Long.parseLong(words[2]),(1L)));

		return ;

	}

}
