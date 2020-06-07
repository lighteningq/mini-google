package edu.upenn.cis455.xpathengine;
import java.util.*;
import java.io.*;
enum Type{contains,text,attr,step};
public class SingleXPath {

	public final String text = "text()";  //6
	public final String contains = "contains(text(),";  //16
	public final String attr = "@";
	
	String xpath;
	int ahead;
	InputStream xpathReader;
	int anchor = -1;  // current index for xpath
	Node root;
	List<Node> parentNodes;
	Node currNode;
	List<Node> currLayerNodes;
	

	
	public SingleXPath(String s) {
		this.xpath = cleanUp(s);
		this.root = new Node("/");
	}
	
	public boolean isValid() {
		if(xpath.charAt(0)!='/') return false;
		if(!checkValidParenthese()) return false;
		
		xpathReader = new ByteArrayInputStream(xpath.getBytes());
		
		try {
			ahead = xpathReader.read();
			anchor++;
			if(ahead!=-1) {
				if(ahead=='/') {
					return parseXPath();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return true;
	}
	
	private boolean parseXPath() throws IOException {

		if(ahead=='/') {
			try {
				ahead = xpathReader.read();
				anchor++;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			if(ahead=='/' || ahead=='*') {
				System.out.println("Invalid XPath with two // or /*");
				return false;
			}
			return parseNewNode(true);
		}

//		if(ahead==']') {
//			anchor++;
//			ahead = xpathReader.read();
//		}
		System.out.println("Inside ParseXPath."+ (char)ahead + "anchor is: "+ xpath.substring(anchor));
		if(ahead==-1) {
		//	if(currLayerNodes!=null) for(Node n: parentNodes) n.addChildren(currLayerNodes);
			return true;  // return true if nothing left
		}
		
		return true;
		
	}
	
	
	private boolean parseNewNode(boolean isDFS) throws IOException {
		String name = findNodeName();
	
		if(name=="" || name==null) return false;
		System.out.println("Node name is: "+ name);
		

		Node n = new Node(name);
	
		
		// for each new layer, we put the children to parents
		if(parentNodes==null || parentNodes.isEmpty()) {
			parentNodes = new ArrayList<>();
			parentNodes.add(root);
		} // add all cur to parents
		
		if(!isDFS) {
			if(currLayerNodes!=null) System.out.println("Current Layer Nodes: "+currLayerNodes.get(0).name);
			
			for(Node each : parentNodes) {
				
				each.addChildren(currLayerNodes);
			}
			if(currLayerNodes!=null) parentNodes = currLayerNodes;
			currLayerNodes = new ArrayList<>();
		}

		currNode = n;
		
		// for test route: /a[element] dfs 
		if(ahead==']') {
			currNode.type = Type.step; 
			System.out.println("encounter ] in parseNode.... "+ "current node is: "+ currNode.name);
			for(Node each : parentNodes) each.children.add(currNode);
			return true;
		}
		
		
//		if(ahead==-1) {
//			System.out.println("here");
//			currNode.type = Type.step; 
//			currLayerNodes.add(currNode);
//			//for(Node each : parentNodes) each.children.add(currNode);
//			return true;
//		}
		else if(ahead=='[') {
			while(ahead=='[') {
				ahead = xpathReader.read();
				anchor++;
				if(ahead==-1) return false;
				if(!parseCondition()) return false;
				// we start parsing bracket here	
			}
			if(ahead==-1) return true;

		}else {

		//	System.out.println("Here");
			currNode.type = Type.step; 
			currLayerNodes.add(currNode);
			return parseXPath();
			
		}
		return true;
		
		
	}
	
	
	private boolean parseCondition() throws IOException{
		int len = xpath.substring(anchor).length();
		
		// text()=...
		if(6<=len && xpath.substring(anchor,anchor+6).equals(text)) {
			System.out.println("Starting Parse text");
			try {	
				anchor = anchor+7;
				for(int i = 0; i<7;i++)
					ahead =xpathReader.read();  // now ahead = condition
				
			}catch (IOException e){
				return false;
			}
			if(ahead!='\"') return false;
			String text = findQuoteName();
			System.out.println("text name is "+ text);
			currNode.type = Type.text;
			currNode.content = text;
			currLayerNodes.add(currNode);
			if(ahead!=']') return false;
			anchor++;
			ahead = xpathReader.read();
			
		}
		
		// contains(....)

		else if(16<=len && xpath.substring(anchor, anchor+16).equals(contains)) {

			try {
				anchor = anchor+16;  // contains(text(), 
				for(int i = 0; i<16;i++)
					ahead =xpathReader.read();  // now ahead = condition
				if(ahead!='\"') return false;
			}catch (IOException e){
				return false;
			}
			
			
			
			String text = findQuoteName();
			System.out.println("contains name: " +text);
			// skip )
			ahead++;
			ahead = xpathReader.read();
			System.out.println((char)ahead);
			currNode.type = Type.contains;
			currNode.content = text;
			currLayerNodes.add(currNode);
			if(ahead!=']') return false;
			anchor++;
			ahead = xpathReader.read();
			
		}
		// @att=
		else if(ahead=='@') {
			System.out.println("Starting Parse Attribute");
			String attr = "";
			ahead++;
			ahead = xpathReader.read();
			while(ahead!='=') {
				attr+=Character.toString((char)ahead);
				anchor++;
				ahead = xpathReader.read();
				if(ahead==-1) return false;
			}
			anchor++;
			ahead = xpathReader.read();
			System.out.println("Attribute is "+(char)ahead);
			if(ahead!='\"') return false;
			
			String text = findQuoteName();
			Map<String, String> map = new HashMap<>();
			map.put(attr,text);
			currNode.type = Type.attr;
			currNode.setContent(map);
			currLayerNodes.add(currNode);
			if(ahead!=']') return false;
			anchor++;
			ahead = xpathReader.read();
			
		// new axis 	
		}else {
			currNode.type = Type.step;
			currLayerNodes.add(currNode);
			System.out.println("Adding Node: ..."+currNode.name+" P:" + parentNodes.get(0).name);
			
//			Node temp = currNode;
//			List<Node> tempLayer = currLayerNodes;
//			List<Node> tempParents = parentNodes;
			if(!parseNewNode(true)) return false;
//			else {
//				currNode = temp;
//				currLayerNodes = tempLayer;
//				parentNodes = tempParents;
//			}
			if(ahead!=']') return false;
			anchor++;
			ahead = xpathReader.read();
		}
		

		
		return true;
		
		
	}
	
	
	private String findQuoteName()throws IOException {
		anchor++;
		ahead = xpathReader.read(); // starting find name
		String res = "";
		while(ahead!='\"') {
			res+=Character.toString((char)ahead);
			anchor++;
			ahead = xpathReader.read(); 
			if(ahead==-1) return null;
		}
		
		anchor++;
		ahead = xpathReader.read(); // skipped double quote
		return res;
		
		
	}
	
	private String findNodeName() throws IOException {
		String res = "";
//		int idx = xpath.substring(anchor).indexOf('[');
//		int idx_slash = xpath.substring(anchor).indexOf('/');
//		if(idx!=-1 && idx_slash!=-1) {
//			if(idx>=idx_slash) return null;
//		}

		if(ahead=='[') return null;
		
		while(ahead!='['&&ahead!='/'&&ahead!=-1 && ahead!=']')
		{
			  res+=Character.toString((char)ahead);
			  ahead=xpathReader.read();
			  anchor++;
		}
		
		// non-escaped chars regex
		if(!res.matches("^(?!(xml|XML))^\\s*([a-zA-Z_])+([a-zA-Z0-9\\-\\._])*\\s*$")) return null;
		
		return res;
	}
	
	
	
	
	private boolean checkValidParenthese() {
		Stack<Character> stack = new Stack<>();
		for(char c: xpath.toCharArray()) {
			if(c==']') {
				if(stack.isEmpty() || stack.peek()!='[') return false;
				else stack.pop();
			}
			if(c==')') {
				if(stack.isEmpty() || stack.peek()!='(')return false;
				else stack.pop();
			}
			
			if(c=='"') {
				if(stack.peek()=='"') stack.pop();
				else stack.push(c);
			}
			
			if(c=='(' || c=='[') stack.push(c);
		}
		
		return stack.isEmpty();
	}
	
	// this cleans up all the whitespaces
	private String cleanUp(String s) {
		boolean betweenQuotes = false;
		char[] arr = s.trim().toCharArray();
		StringBuilder sb = new StringBuilder();
		
		for(int i = 0; i<arr.length;i++) {
			if(betweenQuotes && arr[i]=='\"') {
				sb.append(arr[i]);
				betweenQuotes = false;
			}
			else if(!betweenQuotes && arr[i]=='\"') {
				sb.append(arr[i]);
				betweenQuotes=true;
			}
			else if(betweenQuotes) sb.append(arr[i]);
			else {
				if(arr[i]==' ') continue;
				else sb.append(arr[i]);
			}
		}
		
		return sb.toString();

	}
	
	public Node searchNode(String name, Node root) {
		if(root==null) return null;
		if(root.name == name) return root;
		Node res = null;
		for(Node n: root.children) {
			res = searchNode(name, n);

			if(res!=null) return res;
		}
		
		return res;
	}
	
	public void traverse(Node n) {
		System.out.println("Current Node Value is: "+ n.name);
		System.out.println("Current Node Type is: "+ n.type);
		System.out.println("Current Node Content is: "+ n.content);
		System.out.println(n.name+"---------------------------------------");
		for(Node c: n.children) {
        System.out.println("Current Node Value is: "+ n.name+"  |||children name is "+c.name);
			traverse(c);
		}
		System.out.println(n.name+"------------------------------------------");
		
	}
	
	
}
