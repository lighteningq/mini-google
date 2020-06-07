package edu.upenn.cis455.xpathengine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;



public class Node {
	public String name;
	public Type type;
	public Object content;
	List<Node> children;
	
	public Node() {
		this.children = new ArrayList<>();
	}
	
	public Node(String name) {
		this.name = name;
		this.children = new ArrayList<>();
	}
	
	public void setContent(Object content) {
		if(type==Type.attr) {
			this.content = (Map<String, String>) content;
		}else {
			this.content = (String) content;
		}
	}
	public void addChildren(List<Node> l) {
		if(l==null) return;
		for(Node n: l) {
			this.children.add(n);
		}
	}
}
