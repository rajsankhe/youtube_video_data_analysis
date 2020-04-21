package secondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KeyReducer extends Reducer<CompositeKeyWritable, NullWritable, CompositeKeyWritable,NullWritable>{
    @Override
    protected void reduce(CompositeKeyWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
       try{
           for (NullWritable v:values)
           {
               context.write(key, NullWritable.get());
           }

       } catch(Exception e) {
           System.out.print("reducer errors"+e);
       }

    }
}
