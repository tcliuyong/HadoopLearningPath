package join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by liuyong on 16-4-25.
 */
public class JoinReducer extends Reducer<Text, Text, Text, Text> {
    Logger logger = Logger.getLogger(JoinReducer.class);
        final static String LEFT_FILENAME = "student";
        final static String RIGHT_FILENAME = "class";
        final static String LEFT_FLAG = "l";
        final static String RIGHT_FLAG = "r";
    public void reduce(Text key , Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        List<String> studentClassNames = new ArrayList<>();
        String studentName = "";
        while (iterator.hasNext()){

            String[] infos = iterator.next().toString().split(" ");
            logger.info(key +"\t" + infos[0] + "\t" + infos[1]);
            if(infos[1].equals(LEFT_FLAG)){
                studentName = infos[0];
            }else if(infos[1].equals(RIGHT_FLAG)){
                studentClassNames.add(infos[0]);
            }
            for(int i = 0; i < studentClassNames.size(); i++){
                context.write(new Text(studentName), new Text(studentClassNames.get(i)));
            }
        }
    };

}
