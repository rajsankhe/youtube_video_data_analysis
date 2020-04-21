package secondarysort;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        Job job = Job.getInstance();

        job.setJarByClass(Driver.class);

        job.setGroupingComparatorClass(GroupComparator.class);
        job.setSortComparatorClass(VideoIDCompartor.class);
        job.setPartitionerClass(KeyPartition.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outDir);

        job.setMapperClass(KeyMapper.class);
        job.setReducerClass(KeyReducer.class);

        //job.setMapOutputKeyClass(DateIPObject.class);
        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(1);



        job.setOutputKeyClass(CompositeKeyWritable.class);
        job.setOutputValueClass(NullWritable.class);


        job.waitForCompletion(true);
    }
}
