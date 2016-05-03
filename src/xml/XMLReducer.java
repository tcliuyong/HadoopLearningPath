package xml;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by liuyong on 16-4-25.
 */
public class XMLReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Mapper.Context context)
            throws IOException, InterruptedException {
        String val = "";
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()){
            val = iterator.next().toString();
        }
        context.write(key, new Text("val"));

    }
}
