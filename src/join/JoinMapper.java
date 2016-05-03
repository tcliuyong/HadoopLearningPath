package join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by liuyong on 16-4-25.
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    final static String LEFT_FILENAME = "student";
    final static String RIGHT_FILENAME = "class";
    final static String LEFT_FLAG = "l";
    final static String RIGHT_FLAG = "r";
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
        String fileFlag = null;
        String joinKey = null;
        String joinValue = null;
        //判断记录来自哪个文件
        if(filePath.contains(LEFT_FILENAME)){
            fileFlag = LEFT_FLAG;
            joinKey = value.toString().split(" ")[1];
            joinValue = value.toString().split(" ")[0];
        }else if(filePath.contains(RIGHT_FILENAME)){
            fileFlag = RIGHT_FLAG;
            joinKey = value.toString().split(" ")[0];
            joinValue = value.toString().split(" ")[1];
        }
        context.write(new Text(joinKey), new Text(joinValue + " " + fileFlag));

    };

}
