package topRatedVideos;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class Mapper1 extends Mapper<LongWritable,Text, Text, FloatWritable> {
    final static Logger logger = Logger.getLogger(Mapper1.class);
    private Text video_name = new Text();
    private  FloatWritable rating = new FloatWritable();
    private final static IntWritable one = new IntWritable(1);
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            String line = value.toString();
            String str[] = line.split("\t");
            video_name.set(str[0]);
            if (str[6].matches("\\d+.+")) {
                float f = Float.parseFloat(str[6]);
                rating.set(f);
                context.write(video_name, rating);
            }
            System.out.println(video_name+"       "+ rating);
            } catch(Exception ex){
                System.out.print("exception mapper" + ex);
            }

        }
    }
