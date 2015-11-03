import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class PostCommentPair implements WritableComparable<PostCommentPair> {
	private IntWritable postId;
	private IntWritable postUserId;
	private IntWritable postTypeId;
	private IntWritable parentId;
	private ArrayWritable commentUsers = new ArrayWritable(IntWritable.class);

	public PostCommentPair() {
		this.postId = new IntWritable();
		this.postUserId = new IntWritable();
		this.postTypeId = new IntWritable();
		this.parentId = new IntWritable();
		this.commentUsers.set(new Writable[0]);
	}

	public PostCommentPair(IntWritable postId, IntWritable postUserId,
			IntWritable postTypeId, IntWritable parentId,
			ArrayList<Integer> commentUsers) {
		this.postId = postId;
		this.postUserId = postUserId;
		this.postTypeId = postTypeId;
		this.parentId = parentId;

		IntWritable[] values = new IntWritable[commentUsers.size()];
		int i = 0;
		for (Integer val : commentUsers) {
			values[i] = new IntWritable(val);
			i++;
		}
		this.commentUsers.set(values);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		postId.readFields(in);
		postUserId.readFields(in);
		postTypeId.readFields(in);
		parentId.readFields(in);
		commentUsers.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		postId.write(out);
		postUserId.write(out);
		postTypeId.write(out);
		parentId.write(out);
		commentUsers.write(out);

	}

	public IntWritable getPostId() {
		return postId;
	}

	public void setPostId(IntWritable postId) {
		this.postId = postId;
	}

	public IntWritable getPostUserId() {
		return postUserId;
	}

	public void setPostUserId(IntWritable postUserId) {
		this.postUserId = postUserId;
	}

	public IntWritable getPostTypeId() {
		return postTypeId;
	}

	public void setPostTypeId(IntWritable postTypeId) {
		this.postTypeId = postTypeId;
	}

	public IntWritable getParentId() {
		return parentId;
	}

	public void setParentId(IntWritable parentId) {
		this.parentId = parentId;
	}

	public ArrayWritable getCommentUsers() {
		return commentUsers;
	}

	public void setCommentUsers(ArrayWritable commentUsers) {
		this.commentUsers = commentUsers;
	}

	@Override
	public int compareTo(PostCommentPair o) {
		// TODO Auto-generated method stub
		IntWritable self = this.getPostId();
		return self.compareTo(o.getPostId());
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((commentUsers == null) ? 0 : commentUsers.hashCode());
		result = prime * result + ((postId == null) ? 0 : postId.hashCode());
		result = prime * result
				+ ((postUserId == null) ? 0 : postUserId.hashCode());
		result = prime * result
				+ ((postTypeId == null) ? 0 : postTypeId.hashCode());
		result = prime * result
				+ ((parentId == null) ? 0 : parentId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if (obj instanceof PostCommentPair) {
			PostCommentPair pair = (PostCommentPair) obj;
			return this.getPostId().equals(pair.getPostId());

		}
		return false;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		String output = "";
		output += "Post ID: "+getPostId()+"\t";
		output += "PostType ID: "+getPostTypeId()+"\t";
		output += "PostUser ID: "+getPostUserId()+"\t";
		output += "Parent ID: "+getParentId()+"\t";
//		for (ArrayWritable val: commentUsers) {
//			for (Writable writable: val.get()) {
//				IntWritable userId = (IntWritable) writable;
//				output += "CommentUser ID: "+val.toString()+"\t";
//			}
//		}
		return output;
		
	}

}
