package top5categoriesVideo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class Mapper1 extends Mapper<LongWritable,Text, Text, IntWritable> {
    final static Logger logger = Logger.getLogger(Mapper1.class);
    private Text category = new Text();
    private final static IntWritable one = new IntWritable(1);
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try{
            String line = value.toString();
            String str[]=line.split("\t");
            if(str.length > 5) {
                category.set(str[3]);
            }
            context.write(category, one);
        } catch (Exception ex) {
            System.out.print("exception mapper"+ ex);
        }


    }
}
