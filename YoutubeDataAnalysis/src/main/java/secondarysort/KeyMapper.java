package secondarysort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class KeyMapper extends Mapper<LongWritable,Text, CompositeKeyWritable,NullWritable> {

    private String category ;
    private String videoID ;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try{
            String line = value.toString();
            String str[]=line.split("\t");
            if(str.length > 5) {
                category= str[3];
            }
           videoID=str[0];
            if(category!=null && videoID!=null){
                CompositeKeyWritable catID = new CompositeKeyWritable(category,videoID);

                context.write(catID,NullWritable.get());

            }


        } catch (Exception ex) {
            System.out.print("exception mapper"+ ex);
        }


    }
}
