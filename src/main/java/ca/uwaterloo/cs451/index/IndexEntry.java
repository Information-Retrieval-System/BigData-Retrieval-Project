package ca.uwaterloo.cs451.index;
import java.util.ArrayList;
import java.util.List;

class IndexEntry{
	public String term;
	public int docFreq;
	public List<ca.uwaterloo.cs451.index.DocumentInfo> docInfoList;

	public IndexEntry(String term, int docFreq, List<ca.uwaterloo.cs451.index.DocumentInfo> docInfoList) {
		this.term = term;
		this.docFreq = docFreq;
		this.docInfoList = docInfoList;
	}

	@Override
	public String toString(){
		return "[" + this.term + " " + this.docFreq + " " + this.docInfoList.toString()  + "]";
	}
}
