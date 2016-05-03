package distinct;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by liuyong on 16-5-3.
 */
public class DisReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key , Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(key, new Text(""));
    }
}
