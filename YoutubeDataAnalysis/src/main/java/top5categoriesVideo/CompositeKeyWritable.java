package top5categoriesVideo;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {
	final static Logger logger = Logger.getLogger(CompositeKeyWritable.class);
	private String categoryName;
	private int count;

    public CompositeKeyWritable() {
    }

    public CompositeKeyWritable(String stationName, int count) {
		this.categoryName = stationName;
		this.count = count;
	}

	public int compareTo(CompositeKeyWritable o) {

		return 0;

	}

	public void write(DataOutput out) throws IOException {
		//out.writeUTF(stationName);
		//out.writeUTF(count.toString());
        WritableUtils.writeString(out, categoryName);
        WritableUtils.writeString(out, Integer.toString(count));
		
//		d.writeUTF(stationName);
//		d.writeUTF(count);
	}

	public void readFields(DataInput in) throws IOException {
/*
		stationName = in.readUTF();
		try {
			SimplecountFormat formatter = new SimplecountFormat("dd/MMM/yyyy:HH:mm:ss");
			count = formatter.parse(in.readUTF());
		}
		catch (Exception e){
			e.printStackTrace();
		}*/
		categoryName = WritableUtils.readString(in);
		count = Integer.parseInt(WritableUtils.readString(in));

	}

	public String getStationName() {
		return categoryName;
	}

	public void setStationName(String stationName) {
		this.categoryName = stationName;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "CompositeKeyWritablePart4{" +
				"stationName='" + categoryName + '\'' +
				", count=" + count +
				'}';
	}
}