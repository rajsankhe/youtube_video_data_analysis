package secondarysort;

import org.apache.hadoop.io.WritableComparator;

public class VideoIDCompartor extends WritableComparator {

    public VideoIDCompartor() {
        super(CompositeKeyWritable.class, true);
    }


    @Override
    public int compare(Object a, Object b) {
        CompositeKeyWritable o1= (CompositeKeyWritable)a;
        CompositeKeyWritable o2= (CompositeKeyWritable)b;
        int result= o1.getcategories().compareTo(o2.getcategories());
        if(result==0){
            result = (-1 * o1.getVideoID().compareTo(o2.getVideoID()));
        }
        return result;
    }
}
