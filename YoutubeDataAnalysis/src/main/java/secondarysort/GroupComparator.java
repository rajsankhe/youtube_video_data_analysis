package secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {

    public GroupComparator() {
      super(CompositeKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKeyWritable c = (CompositeKeyWritable)a;
        CompositeKeyWritable d = (CompositeKeyWritable)b;
        return c.getcategories().compareTo(d.getcategories());
    }
}
