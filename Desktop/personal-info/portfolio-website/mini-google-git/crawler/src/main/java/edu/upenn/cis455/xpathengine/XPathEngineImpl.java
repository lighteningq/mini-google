package edu.upenn.cis455.xpathengine;
import java.util.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.helpers.DefaultHandler;
/**
 * this XPath Engine parses XPaths, evaluates them on a given DOM Document object, and checks if the XPaths have valid grammar
 * @author Jingwen-Qiang
 *
 */
public class XPathEngineImpl implements XPathEngine {
	static Logger log = Logger.getLogger(XPathEngineImpl.class);
	List<String> xpaths = new ArrayList<String>();
	List<Boolean> validArr = new ArrayList <Boolean>();
	InputStream xpathReader;
	String curXpath="";
	int forward;
	
	List<Node> curMatch = new ArrayList<Node>();
	
  public XPathEngineImpl() {
    // Do NOT add arguments to the constructor!!
  }
	/**
	 * Sets the XPath expression(s) that are to be evaluated.
	 * @param expressions
	 */
  public void setXPaths(String[] s) {
    /*Store the XPath expressions that are given to this method */
	  xpaths.clear();
	  validArr.clear();

	  for(String xpath : s) {
		  String after =cleanUpWhiteSpaces(xpath);
		  xpaths.add(after);
	  }
	  	
  }
	/**
	 * Returns true if the i.th XPath expression given to the last setXPaths() call
	 * was valid, and false otherwise. If setXPaths() has not yet been called, the
	 * return value is undefined. 
	 * @param i
	 * @return
	 */
  public boolean isValid(int i) {
    /* Check which of the XPath expressions is valid */
	  if(validArr.isEmpty())
		  parseXPaths(null);
    return validArr.get(i);
  }
	/**
	 * evaluates all XPaths against the given document
	 * @returns boolean [] or null if setXPaths has not been called and there are no XPaths to evaluate
	 */
  public boolean[] evaluate(Document d) { 
    /* if allXPaths is empty then setXPaths was not called*/
	if(xpaths.isEmpty())
		return null;
    boolean [] evaluatePaths = parseXPaths(d);
    return evaluatePaths;
  }
  
	/**
	 * Returns true if the implementation is a SAX rather than DOM parser.
	 * i.e., the caller should call evaluateSAX rather than evaluate.
	 * 
	 * @return
	 */
  @Override
  public boolean isSAX() {
  	// TODO Auto-generated method stub
  	return false;
  }
  
	/**
	 * SAX parser evaluation. (Optional extra credit.)
	 * 
	 * Takes a stream as input, as well as a Handler produced by the
	 * XPathEngineFactory.   Returns an array of the same length as the 'expressions'
	 * argument to setXPaths(), with the i.th element set to true if the document 
	 * matches the i.th XPath expression, and false otherwise. If setXPaths() has not
	 * yet been called, the return value is undefined.
	 * 
	 * @param document Document stream
	 * @param handler SAX handler implementation (from factory)
	 * @return bit vector of matches
	 */
  @Override
  public boolean[] evaluateSAX(InputStream document, DefaultHandler handler) {
  	// TODO Auto-generated method stub
  	return null;
  }
  
  

	private boolean checkValidParenthese(String xpath) {
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
	private String cleanUpWhiteSpaces(String s) {
		s.replace("&quot;", "\"");
		boolean betweenQuotes = false;
		char[] arr = s.trim().toCharArray();
		StringBuilder sb = new StringBuilder();
		
		for(int i = 0; i<arr.length;i++) {
			if(betweenQuotes) {
				if(arr[i]=='\"') {
					betweenQuotes=false;
					sb.append(arr[i]);
				}else {
					sb.append(arr[i]);
				}
			}else {
				if(arr[i]=='\"') {
					betweenQuotes=true;
					sb.append(arr[i]);
				}else {
					
					if(arr[i]==' ')continue;
					else sb.append(arr[i]);
				}
				
				
			}
		}
	//	System.out.println("-------------"+sb.toString()+"-----------");
		
		return sb.toString();

	}
	
	  private String replaceQuote(String text)
	  {
		  text.replace("&quot;", "\"");
		  return text;
	  }
  /**
   * sets the index of the associated xpath in the isValidXPathArray to be true or false to mean valid or invalid
   * @param isValid
   * @param currentPath
   */
  private void setcurPathValid(boolean isValid, String currentPath)
  {
	  validArr.add(xpaths.indexOf(currentPath), isValid);
  }
 
  /**
   * parses all XPaths and sets if they are Valid or not. If a Document is given, it evaluates each XPath on the Document
   * If the Document is null, then the XPaths are just parsed, and not compared to a Document.
   * @param d
   * @return a boolean array of the result of matched or not matched xpaths
   */
  private boolean[] parseXPaths(Document d)
  {
	  
	  boolean [] matches = new boolean [xpaths.size()];
	  for(String xpath : xpaths)
	  {
		  if(d!=null)curMatch.add(d);
		  
		  if(!checkValidParenthese(xpath)) {
			  matches[xpaths.indexOf(xpath)] = false;
			  continue;
		  }
		  
		  curXpath = xpath;
		  xpathReader = new ByteArrayInputStream(xpath.getBytes());
		  try {
			forward = xpathReader.read();

			if(forward!=-1)
			{
				parse();

				if(!curMatch.isEmpty())
					matches[xpaths.indexOf(xpath)] = true;
				else
					matches[xpaths.indexOf(xpath)] = false;
			}
		  } catch (IOException e) {
			e.printStackTrace();
		  } catch (ParseException e) {
			  e.printStackTrace();
			setcurPathValid(false, xpath);
			log.debug("set "+xpath+" invalid");
			curMatch.clear();
			matches[xpaths.indexOf(xpath)] = false;
		  }
		  
	  }
	  return matches;
	     
  }
  /**
   * recursive function to parse axis step pairs of an XPath
   * @throws IOException
   * @throws ParseException
   */
  private void parse() throws IOException, ParseException
  {

	  //parse the axis then call parseStep, then recursively parse the next axis step pair
	  if(forward=='/')
	  {
		  forward=xpathReader.read();
		  if(forward =='/'||forward == '*') throw new ParseException("", 0);
		  parseStep();
		  parse();  //recursive parse
	  }
	  else if(forward == -1)
	  {
		  setcurPathValid(true, curXpath);
		  log.debug("set "+curXpath+" valid");
		  return;
	  }
	  else throw new ParseException("", 0);
  }
  /**
   * parse the step component of the XPath which is made up of nodename([ test ])
   * @throws ParseException 
   * @throws IOException 
   */
  private void parseStep() throws ParseException, IOException
  {

	  //if there is not a condition on this node or after all conditions have been parsed,
	  //add all of the possibleDOMMatches existing nodes' children nodes to the list of possibles
	  updateChildren();
	  String nodename="";
	  nodename = getNodeName();
	  nodename=nodename.trim();

	  

	  findDocMatch(nodename);
	  
	  if(forward == -1)
	  {
		  return;
	  }//if there is a condition on this node
	  else if(forward == '[')
	  {
		  forward=xpathReader.read();
		  List<Node> copy = new ArrayList<Node>();
		  for(Node item: curMatch) copy.add(item);
		  if(!curMatch.isEmpty()) copy.addAll(curMatch);
		  List<Node> matchesAfter = parseTestCond(copy);
		  
		  removeAndUpdate(matchesAfter);


		  //above parsed the first condition, now this will parse all of the rest of the conditions for this step
		  while(forward=='[')
		  {
	
			  copy.clear();
			  for(Node item: curMatch) copy.add(item);

			  forward=xpathReader.read();
			  matchesAfter = parseTestCond(copy);
			  
			  removeAndUpdate(matchesAfter);
			 

		  }
		  if(forward!=-1 && forward!='/')throw new ParseException("", 0);
	  }
	  
  }
 

  /**
   * read in until the bracket or slash and that represents the nodename
   * @return
   * @throws IOException
 * @throws ParseException 
   */
  private String getNodeName() throws IOException, ParseException
  {

	  String nodename = "";
	  while(forward!='['&&forward!='/'&&forward!=-1)
	  {
		  nodename+=Character.toString((char)forward);
		  forward=xpathReader.read();
	  }

	  if(!nodename.matches("^(?!(xml|XML))^\\s*([a-zA-Z_])+([a-zA-Z0-9\\-\\._])*\\s*$"))throw new ParseException("", 0);
	
	  return nodename;
  }
  /**
   * read till ]
   * @return conditionName
   * @throws IOException
 * @throws ParseException 
   */
  private String findCondName() throws IOException, ParseException
  {
	  String conditionName = "";
	  while(forward!=']'&&forward!='"'&&forward!='['&&forward!='/'&&forward!=-1)
	  {
		  conditionName+=Character.toString((char)forward);
		  forward=xpathReader.read();
	  }
	  
	  if(!conditionName.matches("^(?!(xml|XML))^\\s*([a-zA-Z_])+([a-zA-Z0-9\\-\\._])*\\s*$")&&!conditionName.matches("^(\\s*contains\\s*\\(\\s*text\\s*\\(\\s*\\)\\s*,)\\s*$")&&!conditionName.matches("^(\\s*text\\s*\\(\\s*\\)\\s*=)\\s*$")&&!conditionName.matches("^\\s*@\\s*([a-zA-Z\\_\\:]+[\\-a-zA-Z0-9\\_\\:\\.]*)\\s*=\\s*$"))throw new ParseException("", 0);
	  

	  return conditionName;
  }
  /**
   * parse till )]
   * nodename[test]
   *
   * @return after
   * @throws IOException
 * @throws ParseException 
   */
  private String parseText() throws IOException, ParseException
  {

	  String after = "";
	  while(forward!=']'&&forward!=-1)
	  {
		  after+=Character.toString((char)forward);
		  forward=xpathReader.read();
	  }
	  if(forward == -1) throw new ParseException("", 0);
	  
	  return after;
  }
  /**
   * recursive function to parse the condition that follows a nodename, e.g. nodename[condition]. This
   * is equivalent to the test token mentioned in the writeup.
   *
 * @throws ParseException 
 * @throws IOException 
   */
  private List<Node> parseTestCond(List<Node> copy) throws ParseException, IOException
  {

	  String conditionName= "";
	  String afterComp = "";
	  String quoteText = "";
	  
	  conditionName = findCondName();
	  conditionName = conditionName.trim();

	  if(forward==']')
	  {
		  copy = updateChildren(conditionName, copy);
		  forward=xpathReader.read();
		  return copy;
	  }
	  else if(forward=='/')
	  {
		  copy = updateChildren(conditionName, copy);
		  forward=xpathReader.read();
		  copy = parseTestCond(copy);
		  if(forward==-1)
			  return copy;
	  }
	  else if(forward=='"')
	  {
		  forward=xpathReader.read();
		  quoteText = parseComparisonString();
		  
		  if(conditionName.matches("^(\\s*contains\\s*\\(\\s*text\\s*\\(\\s*\\)\\s*,)\\s*$"))
		  {
			  copy = updateContainsMatches(quoteText, copy);
		  }
		  else if(conditionName.matches("^(\\s*text\\s*\\(\\s*\\)\\s*=)\\s*$"))
			  copy = updateConditionMatchesWithExactText(quoteText, copy);
		  else if(conditionName.matches("^\\s*@\\s*([a-zA-Z\\_\\:]+[\\-a-zA-Z0-9\\_\\:\\.]*)\\s*=\\s*$"))
		  {
			  //remove @
			  conditionName = conditionName.substring(1);
			  conditionName = conditionName.substring(0, conditionName.indexOf('='));
			  conditionName.trim();
			  updateConditionMatchesWithAttribute(conditionName, quoteText, copy);
		  }
		  afterComp = parseText();
		  afterComp = afterComp.trim();
	
		  
		  /*if there were characters after the end of the end quote then it must have been 
		  * a contains() condition or an error
		  */
		  if(!afterComp.equals(""))
		  {
			  if(conditionName.startsWith("contains("))
			  {
				  if(!afterComp.equals(")"))
				  {
					  throw new ParseException("", 0);
				  }		  
			  }
			  else
			  {
				  throw new ParseException("", 0);
			  }
		  }
	  }
	  else if(forward=='[')
	  {
		  forward=xpathReader.read();
		  copy = updateChildren(conditionName, copy);
		  List<Node>updateMatch = parseTestCond(copy);
		
		  
		  
		  while(forward=='['||forward=='/')
		  {
			  if(forward == '/')
			  {
				  // return immediately if no matches
				  if(updateMatch.isEmpty())
				  {
					  return updateMatch;
				  }
				  else
				  {
					  forward=xpathReader.read();
					  return parseTestCond(copy);
				  }
			  }
			  else
			  {
				  copy = removeUnmatched(copy, updateMatch);

				  forward=xpathReader.read();
				  updateMatch = parseTestCond(copy);

				
			  }
			  
		  }
		  copy=updateMatch;
		  }
	  
	  if(forward != ']')
	  {

		  throw new ParseException("", 0);
	  }
	
	  forward=xpathReader.read(); // skip ]
	  return copy;
  }
  /**
   * parse the string  "text()="..."", 
   * "contains(text(), "...")", and "@attname = "...""
 * @throws IOException 
 * @throws ParseException 
   */
  private String parseComparisonString() throws IOException, ParseException
  {
	  String res="";
	  while(forward!='"'&&forward!=-1)
	  {
		  if(forward=='\\')// \ character
		  {
			  int next = xpathReader.read();
			  if(next =='"')
			  {
				  res+=Character.toString((char)next);
				  forward = xpathReader.read();
			  }
			  else
			  {
				  res+=Character.toString((char)forward);
				  forward = next;
			  }
			  
		  }
		  else
		  {
			  res+=Character.toString((char)forward);
			  forward=xpathReader.read();
		  }
	  }

	  if(forward == -1)throw new ParseException("", 0);
	  
	  forward=xpathReader.read();
	  return res;
	  
  }
  
  /**
   * find doc match according to nodename
   * @param nodename
   */
  private void findDocMatch(String nodename)
  {
	  List<Node> newMatch = new ArrayList<Node>();
	  for(Node cur : curMatch)
	  {
		  if(cur.getNodeName().equals(nodename))
			  newMatch.add(cur);
	  }
	  curMatch = newMatch;
  }

  /**
   * update children after comp
   */
  private List<Node> updateChildren(String compName, List<Node> curMatch)
  {
	  List<Node> res = new ArrayList<Node>();
	  for(Node currentNode : curMatch)
	  {
		  NodeList children = currentNode.getChildNodes();
		  for(int i=0; i<children.getLength(); i++)
		  {
			  if(children.item(i).getNodeType()==Node.ELEMENT_NODE)
			  {
				  if(children.item(i).getNodeName().equals(compName))
					  res.add(children.item(i));
			  }
		  }
	  }
	  return res;
  }
  /**
   * find doc matches contains(text(), "")
   * @param quoteName
   * @param curMatch
   * @return updated_matches
   */
  private List<Node> updateContainsMatches(String quoteName, List<Node>curMatch)
  {
	  List<Node> res = new ArrayList<Node>();
	  for(Node cur : curMatch)
	  {
		  NodeList children = cur.getChildNodes();
		  for(int i = 0; i<children.getLength();i++)
		  {
			  if(children.item(i).getNodeType()==Node.TEXT_NODE)
			  {
				  
				  String text = replaceQuote(children.item(i).getNodeValue());
				  if(text.contains(quoteName))
				  {
					  
					  if(!res.contains(cur))
						  res.add(cur);
				  }
					  
			  }
		  }
	  }
	  return res;
  }
  
  /**
   * This method removes nodes from the global list if they are not ancestor nodes of the nodes in the passed in ArrayList, 
   * meaning the node did not match the condition and is no longer a possible match.
   * @param matchAfterCond
   */
  private void removeAndUpdate(List<Node> matchAfterCond)
  {
	  List<Node> res = new ArrayList<Node>();
	  for(Node curDec : matchAfterCond)
	  {
		 
		  for(Node cur : curMatch)
		  {

			  if(cur.isEqualNode(curDec)&&!res.contains(cur))
				  res.add(cur);
				
			  
			  if((curDec.compareDocumentPosition(cur)== (Node.DOCUMENT_POSITION_CONTAINS+Node.DOCUMENT_POSITION_PRECEDING)))
			  {
			
				  if(!res.contains(cur))
					  res.add(cur);
			  }

		  }
	  }
	  curMatch = res;
  }


  /**
   * compares the given string to the text node children of all nodes in the arraylist passed in
   * if the string exactly matches the text in the text node, it is added as a possible match and the array is returned
   * @param quoteText
   * @param curMatch
   * @returns the new list of possible matches
   */
  private List<Node> updateConditionMatchesWithExactText(String quoteText, List<Node>curMatch)
  {
	  List<Node> res = new ArrayList<Node>();
	  for(Node currentNode : curMatch)
	  {
		  NodeList children = currentNode.getChildNodes();
		  for(int i = 0; i<children.getLength();i++)
		  {
			 
			  if(children.item(i).getNodeType()==Node.TEXT_NODE)
			  {
				  
				  String nodeVal = replaceQuote(children.item(i).getNodeValue());
				  if(nodeVal.equals(quoteText))
				  {
					  
					  if(!res.contains(currentNode))
						  res.add(currentNode);
				  }
					  
			  }
		  }
	  }
	  return res;
  }
  /**
   * checks that the Nodes with attribute and update the list
   * @param attr
   * @param text
   * @param copy
   * @return updated list
   */
  private List<Node>updateConditionMatchesWithAttribute(String attr, String text, List<Node> copy)
  {
	  List<Node> res = new ArrayList<Node>();
	  for(Node cur : copy)
	  {
		  NamedNodeMap attrs = cur.getAttributes();
		  if(attrs!=null)
		  {
			  Node selected = attrs.getNamedItem(attr);
			  if(selected!=null&&selected.getNodeValue().equals(text))
				  res.add(cur);
		  }
	  }
	  return res;
  }
  /**
   * update nodes
   * 
   * @param matchesBefore
   * @param matchesAfter
   * @return updated list
   */
  private List<Node> removeUnmatched(List<Node> matchesBefore, List<Node> matchesAfter)
  {
	  ArrayList<Node> res = new ArrayList<Node>();
	  for(Node curDesc : matchesAfter)
	  {
		  for(Node cur : matchesBefore)
		  {
			  if(cur.isEqualNode(curDesc)&&!res.contains(cur))
				  res.add(cur);
				
			  if((curDesc.compareDocumentPosition(cur)== (Node.DOCUMENT_POSITION_CONTAINS+Node.DOCUMENT_POSITION_PRECEDING)))
			  {
			
				  if(!res.contains(cur))
					  res.add(cur);
			  }
			 
		  }
	  }
	  return res;
	  
  }
  
  /**
   * updates the possible matches in all children
   * so that all the childNodes can be compared to the next step of the XPath query
   */
  private void updateChildren()
  {
	  List<Node> newPossibleMatches = new ArrayList<Node>();
	  for(Node child : curMatch)
	  {
		  NodeList children = child.getChildNodes();
		  for(int i=0; i<children.getLength(); i++)
		  {
			  if(children.item(i).getNodeType()==Node.ELEMENT_NODE)
			  {
				  newPossibleMatches.add(children.item(i));
		
			  }
		  }
	  }
	  curMatch = newPossibleMatches;
  }


        
}
