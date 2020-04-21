package topRatedVideos;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class Reducer1 extends Reducer<Text, FloatWritable,Text,FloatWritable>{
    public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
        int sum = 0;
        int l=0;
        for (FloatWritable val : values) {
            l+=1;
            sum += val.get();
        }
        sum=sum/l;
        context.write(key, new FloatWritable(sum));
    }
}

