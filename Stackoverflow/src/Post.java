
public class Post {
	private int ownerUserId;
	private int postId;
	private int postIdType;
	private int parentId;
	
	
	public int getOwnerUserId() {
		return ownerUserId;
	}
	public void setOwnerUserId(int ownerUserId) {
		this.ownerUserId = ownerUserId;
	}
	public int getPostId() {
		return postId;
	}
	public void setPostId(int postId) {
		this.postId = postId;
	}
	public int getPostIdType() {
		return postIdType;
	}
	public void setPostIdType(int postIdType) {
		this.postIdType = postIdType;
	}
	public int getParentId() {
		return parentId;
	}
	public void setParentId(int parentId) {
		this.parentId = parentId;
	}
	
	public Post(int ownerUserId, int postId, int postIdType, int parentId) {
		super();
		this.ownerUserId = ownerUserId;
		this.postId = postId;
		this.postIdType = postIdType;
		this.parentId = parentId;
	}
}
