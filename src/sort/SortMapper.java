package sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Waitable;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.IOException;

/**
 * Created by tcliuyong on 16-5-3.
 */
public class SortMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private static IntWritable data = new IntWritable();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.equals(""))
            data.set(Integer.parseInt(line));
        System.out.println(data.get());
        context.write(data, new IntWritable(1));
    }
}
