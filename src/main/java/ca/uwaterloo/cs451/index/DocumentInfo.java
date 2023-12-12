package ca.uwaterloo.cs451.index;
import java.util.ArrayList;
import java.util.List;
class DocumentInfo {
	public String docid;
	public int tf;
	public int doclen;

	public DocumentInfo(String docid, int tf, int doclen) {
		this.docid = docid;
		this.tf = tf;
		this.doclen = doclen;
	}

	@Override
	public String toString(){
		return "[" + this.docid + " " + this.tf + " " + this.doclen  + "]";
	}
}
