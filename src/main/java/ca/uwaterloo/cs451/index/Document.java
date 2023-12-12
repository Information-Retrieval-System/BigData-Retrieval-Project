package ca.uwaterloo.cs451.index;
import java.util.ArrayList;
import java.util.List;
class Document {
	public String docid;
	public List<String> content;

	public Document(String docid, List<String> content) {
		this.docid = docid;
		this.content = content;
	}
}
