package secondarysort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKeyWritable implements Writable,WritableComparable<CompositeKeyWritable> {

	private String categories;
	private String videoID;

	public CompositeKeyWritable(){

	}

	public CompositeKeyWritable(String ip, String videoID){

		this.categories=ip;
		this.videoID=videoID;
	}

	public int compareTo(CompositeKeyWritable o) {

		int result =categories.compareTo(o.categories);
		if (result==0){
			result=videoID.compareTo(o.videoID);
		}
		return result;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(categories);
		out.writeUTF(videoID);
//		WritableUtils.writeString(d, categories);
//		WritableUtils.writeString(d, videoID.toString());

//		d.writeUTF(categories);
//		d.writeUTF(videoID);
	}

	public void readFields(DataInput in) throws IOException {

		categories = in.readUTF();
		try {
			videoID = in.readUTF();
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}

	public String getcategories() {
		return categories;
	}


	public void setcategories(String categories) {
		this.categories = categories;
	}

	public String getVideoID() {
		return videoID;
	}

	public void setVideoID(String videoID) {
		this.videoID = videoID;
	}

	@SuppressWarnings("deprecation")
	public String toString(){
		return (new StringBuilder().append(categories).append("\t").append(videoID).toString());
	}

}