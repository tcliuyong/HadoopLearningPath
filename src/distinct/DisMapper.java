package distinct;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by liuyong on 16-5-3.
 */
public class DisMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static Text line = new Text();
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        line = value;
        context.write(line, new Text(""));
    }
}
