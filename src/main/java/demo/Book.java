package demo;

public class Book {
	private String name;
	private String authorName;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAuthorName() {
		return authorName;
	}
	public void setAuthorName(String authorName) {
		this.authorName = authorName;
	}
	@Override
	public String toString() {
		
		return "Book{name: "+name+", author:" + authorName +"}";
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (!(obj instanceof Book))
			return false;
		
		Book other = (Book) obj;
		if (this.name == null) 
			return other.name == null;
		
		return this.name.equals(other.name);
		
	}
	
	@Override
	public int hashCode() {
		return name.hashCode();
	}
}
