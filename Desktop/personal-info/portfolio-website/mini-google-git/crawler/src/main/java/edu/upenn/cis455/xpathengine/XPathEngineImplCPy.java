package edu.upenn.cis455.xpathengine;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.*;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.helpers.DefaultHandler;
/**
 * MS2..Evaluates XPath expressions
 * @Checks if XPath is valid and put available 
 * @author Jingwen Qiang
 *
 */
public class XPathEngineImplCPy implements XPathEngine {
	List<String> xpaths = new ArrayList<String>();
	List<Boolean> validXPath = new ArrayList <Boolean>();
	InputStream xpathReader;
	String curpath="";
	int frontbyte;
	
	List<Node> curMatches = new ArrayList<Node>();
	
  public XPathEngineImplCPy() {
    // Do NOT add arguments to the constructor!!
  }
	/**
	 * Sets the XPath expression(s) that are to be evaluated.
	 * @param expressions
	 */
  public void setXPaths(String[] s) {
    /*Store the XPath expressions that are given to this method */
	  xpaths.clear();
	  validXPath.clear();
	  for(String xpath : s)xpaths.add(xpath.trim());
	  for(String xpath: s) if(!checkValidParenthese(xpath)) setValid(false,xpath);
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
	  if(validXPath.isEmpty())
		  parseXPaths(null);
    return validXPath.get(i);
  }
	/**
	 * evaluates all XPaths against the given document
	 * @returns boolean [] or null if setXPaths has not been called and there are no XPaths to evaluate
	 */
  public boolean[] evaluate(Document d) { 
	if(xpaths.isEmpty())return null;
    boolean [] evaluatePaths = parseXPaths(d);
    return evaluatePaths;
  }
  /**
   * sets the index of the associated xpath in the isValidXPathArray to be true or false to mean valid or invalid
   * @param isValid
   * @param xpath
   */
  private void setValid(boolean isValid, String xpath)
  {
	  validXPath.add(xpaths.indexOf(xpath), isValid);
  }
 
  /**
   * parses all xpath and find doc match
   * @param doc
   * @return boolean[] for valid
   */
  private boolean[] parseXPaths(Document doc)
  {
	  
	  boolean [] XPathMatches = new boolean [xpaths.size()];
	  for(String currentPath : xpaths)
	  {
		  if(doc!=null)
			  curMatches.add(doc);
		  curpath = currentPath;
		  xpathReader = new ByteArrayInputStream(currentPath.getBytes());
		  try {
			frontbyte = xpathReader.read();

			if(frontbyte!=-1)
			{
				parse();
				if(!curMatches.isEmpty())
					XPathMatches[xpaths.indexOf(currentPath)] = true;
				else
					XPathMatches[xpaths.indexOf(currentPath)] = false;
			}
		  } catch (IOException e) {
			e.printStackTrace();
		  } catch (ParseException e) {
			  e.printStackTrace();
			setValid(false, currentPath);
			System.out.println("set "+currentPath+" invalid");
			curMatches.clear();
			XPathMatches[xpaths.indexOf(currentPath)] = false;
		  }
		  
	  }
	  return XPathMatches;
	     
  }
  
  private boolean checkValidParenthese(String xpath) {
	  Stack<Character> s = new Stack<>();
	  for(char c: xpath.toCharArray()) {
		  if(c==']') {
			  if(s.isEmpty() || s.peek()!='[') return false;
			  s.pop();
		  }
		  if(c==')') {
			  if(s.isEmpty() || s.peek()!='(') return false;
			  s.pop();
		  }
		  if(c=='[' || c=='(') {
			  s.push(c);
		  }
		  if(c=='"') {
			  if(!s.isEmpty() && s.peek()=='"') s.pop();
			  else s.push(c);
		  }
	  }
	  return s.isEmpty();
  }
  /**
   * parse axis step pairs of an XPath
   */
  private void parse() throws IOException, ParseException
  {
	  if(frontbyte=='/')
	  {
		  frontbyte=xpathReader.read();
		  if(frontbyte =='/'||frontbyte == '*')
			  throw new ParseException("Invalid XPath found",1);
		  parseStep();
		  parse();
	  }
	  else if(frontbyte == -1)
	  {
		  setValid(true, curpath);
		  return;
	  }
	  else
		  throw new ParseException("", 1);
  }
  /**
   * parse nodename[test] 
   */
  private void parseStep() throws ParseException, IOException
  {

	  updatePossibleMatchesWithChildNodes();
	  String nodename="";
	  nodename = getNodeName();
	  nodename=nodename.trim();


	  findDomMatch(nodename);
	  
	  if(frontbyte == -1)return;
	  else if(frontbyte == '[')
	  {
		  frontbyte=xpathReader.read();
		  ArrayList<Node> copy = new ArrayList<Node>();
		  for(Node item: curMatches) copy.add(item);
		  if(!curMatches.isEmpty())
			  copy.addAll(curMatches);
		  List<Node> matchesAfter = parseCondition(copy);

		  
		  removeAfterCond(matchesAfter);

		  cleanup();
	
		  while(frontbyte=='[')
		  {

	
			  copy.clear();
			  for(Node item: curMatches) copy.add(item);
			  frontbyte=xpathReader.read();
			  matchesAfter = parseCondition(copy);
			  removeAfterCond(matchesAfter);
			  cleanup();
		  }

		  throw new ParseException("Invalid XPath found",1);
	  }
	  
  }
 
  /**
   * removes all white space
   */
  private void cleanup() throws IOException
  {
	  while(frontbyte==' ')
	  {
		  frontbyte=xpathReader.read();
	  }
	  //System.out.println("removeAllWhiteSpace complete, current lookaheadByte = "+lookaheadByte);
  }
  /**
   * read in until the bracket or slash and that represents the nodename
   * @return
   * @throws IOException
 * @throws ParseException 
   */
  private String getNodeName() throws IOException, ParseException
  {
	  //System.out.println("in getNodeName");
	  String nodename = "";
	  while(frontbyte!='['&&frontbyte!='/'&&frontbyte!=-1)
	  {
		  nodename+=Character.toString((char)frontbyte);
		  frontbyte=xpathReader.read();
	  }
	  //System.out.println(nodename);
	  //regular regex with non escaped characters: ^(?!(xml|XML))^\s*([a-zA-Z_])+([a-zA-Z0-9\-\._])*\s*$
	  if(!nodename.matches("^(?!(xml|XML))^\\s*([a-zA-Z_])+([a-zA-Z0-9\\-\\._])*\\s*$"))
	  {

		  throw new ParseException("Invalid XPath found",1);
	  }
	  return nodename;
  }
  /**
   * read in until the open or close bracket or quote
   * the string read in represents the condition name inside nodename[test]
   * where test is made up of conditionName and Comparison String
   * e.g. 'text() = ', or 'contains(text(), ', or '@attname = '
   * if the stream ends without hitting one of those characters then throw an exception
   * @return conditionName
   * @throws IOException
 * @throws ParseException 
   */
  private String findCondName() throws IOException, ParseException
  {
	  //System.out.println("in getConditionName");
	  String name = "";
	  while(frontbyte!=']'&&frontbyte!='"'&&frontbyte!='['&&frontbyte!='/'&&frontbyte!=-1)
	  {
		  name+=Character.toString((char)frontbyte);
		  frontbyte=xpathReader.read();
	  }
	  if(!name.matches("^(?!(xml|XML))^\\s*([a-zA-Z_])+([a-zA-Z0-9\\-\\._])*\\s*$")&&!name.matches("^(\\s*contains\\s*\\(\\s*text\\s*\\(\\s*\\)\\s*,)\\s*$")&&!name.matches("^(\\s*text\\s*\\(\\s*\\)\\s*=)\\s*$")&&!name.matches("^\\s*@\\s*([a-zA-Z\\_\\:]+[\\-a-zA-Z0-9\\_\\:\\.]*)\\s*=\\s*$"))
	  {
		  throw new ParseException("invalid characters in condition", 1);
	  }

	  return name;
  }

  private String parseAfterComp() throws IOException, ParseException
  {

	  String res = "";
	  while(frontbyte!=']'&&frontbyte!=-1)
	  {
		  res+=Character.toString((char)frontbyte);
		  frontbyte=xpathReader.read();
	  }
	  if(frontbyte == -1) throw new ParseException("", 1);
	  
	  return res;
  }
  /**
   * recursive function to parse the condition that follows a nodename, e.g. nodename[condition]. This
   * is equivalent to the test token mentioned in the writeup.
   *
 * @throws ParseException 
 * @throws IOException 
   */
  private List<Node> parseCondition(List<Node> possibleConditionMatches) throws ParseException, IOException
  {
	  String condition= "";
	  String afterComp = "";
	  String quoteName = "";
	  
	  condition = findCondName();
	  condition = condition.trim();
	  if(frontbyte==']')
	  {
		  possibleConditionMatches = updateConditionMatchesWithMatchingChildNodes(condition, possibleConditionMatches);
		  frontbyte=xpathReader.read();
		  return possibleConditionMatches;
	  }
	  else if(frontbyte=='/')
	  {
		  possibleConditionMatches = updateConditionMatchesWithMatchingChildNodes(condition, possibleConditionMatches);
		  frontbyte=xpathReader.read();
		  possibleConditionMatches = parseCondition(possibleConditionMatches);
		  if(frontbyte==-1)
			  return possibleConditionMatches;
	  }
	  else if(frontbyte=='"')
	  {
		  frontbyte=xpathReader.read();
		  quoteName = parseComparisonString();

		  if(condition.matches("^(\\s*contains\\s*\\(\\s*text\\s*\\(\\s*\\)\\s*,)\\s*$"))
		  {
			  possibleConditionMatches = updateConditionMatchesWithContainsText(quoteName, possibleConditionMatches);
		  }
		  else if(condition.matches("^(\\s*text\\s*\\(\\s*\\)\\s*=)\\s*$"))
			  possibleConditionMatches = updateConditionMatchesWithExactText(quoteName, possibleConditionMatches);
		  else if(condition.matches("^\\s*@\\s*([a-zA-Z\\_\\:]+[\\-a-zA-Z0-9\\_\\:\\.]*)\\s*=\\s*$"))
		  {
			  
			  condition = condition.substring(1);
			  condition = condition.substring(0, condition.indexOf('='));
			  condition.trim();
			  updateConditionMatchesWithAttribute(condition, quoteName, possibleConditionMatches);
		  }
		  afterComp = parseAfterComp();
		  afterComp = afterComp.trim();

		  
		  /*if there were characters after the end of the end quote then it must have been 
		  * a contains() condition or an error
		  */
		  if(!afterComp.equals(""))
		  {
			  if(condition.startsWith("contains("))
			  {
				  if(!afterComp.equals(")"))
				  {
					  throw new ParseException("", 0);
				  }		  
			  }
			  else
			  {
				  throw new ParseException("Invalid XPath found",1);
			  }
		  }
	  }
	  else if(frontbyte=='[')
	  {
		  frontbyte=xpathReader.read();
		  possibleConditionMatches = updateConditionMatchesWithMatchingChildNodes(condition, possibleConditionMatches);
		  List<Node>resultantPossibleConditionMatches = parseCondition(possibleConditionMatches);
		  cleanup();
		  
		  while(frontbyte=='['||frontbyte=='/')
		  {
			  if(frontbyte == '/')
			  {
				  //if all of the sibling conditions (e.g. [][]) at this level result in no matches, then return the empty matches all the way up 
				  if(resultantPossibleConditionMatches.isEmpty())
				  {
					  //lookaheadByte=currentXPathIn.read();
					  //return parseCondition(resultantPossibleConditionMatches);
					  return resultantPossibleConditionMatches;
				  }
				  else
				  {
					  frontbyte=xpathReader.read();
					  return parseCondition(possibleConditionMatches);
				  }
			  }
			  else
			  {
				  possibleConditionMatches = removeNodesNotMatchingIntermediateCondition(possibleConditionMatches, resultantPossibleConditionMatches);
				  
				  frontbyte=xpathReader.read();
				  resultantPossibleConditionMatches = parseCondition(possibleConditionMatches);
				  cleanup();
				
			  }
			  
		  }
		  possibleConditionMatches=resultantPossibleConditionMatches;
		  }
	  
	  if(frontbyte != ']')
	  {

		  throw new ParseException("", 0);
	  }
	  
	  frontbyte=xpathReader.read(); //skip ]
	  return possibleConditionMatches;
  }
  /**
   * parse the string that would be the comparison for conditions like "text()="..."", 
   * "contains(text(), "...")", and "@attname = "...""
 * @throws IOException 
 * @throws ParseException 
   */
  private String parseComparisonString() throws IOException, ParseException
  {
	  String comparisonString="";
	  while(frontbyte!='"'&&frontbyte!=-1)
	  {
		  if(frontbyte==92)// \ character
		  {
			  int nextLookaheadByte = xpathReader.read();
			  if(nextLookaheadByte =='"')
			  {
				  comparisonString+=Character.toString((char)nextLookaheadByte);
				  frontbyte = xpathReader.read();
			  }
			  else
			  {
				  comparisonString+=Character.toString((char)frontbyte);
				  frontbyte = nextLookaheadByte;
			  }
			  
		  }
		  else
		  {
			  comparisonString+=Character.toString((char)frontbyte);
			  frontbyte=xpathReader.read();
		  }
	  }

	  if(frontbyte == -1)throw new ParseException("", 1);
	  
	  frontbyte=xpathReader.read();
	  return comparisonString;
	  
  }
  
  /**
   * Compares the current nodename with the currentPossibleDOMMatches list of nodes
   *  to check for a match, if there is a match, the node stays in the list, if not, it is removed
   * @param nodename
   * @param e
   */
  private void findDomMatch(String nodename)
  {
	  ArrayList<Node> newPossibleMatches = new ArrayList<Node>();
	  for(Node currentNode : curMatches)
	  {
		  if(currentNode.getNodeName().equals(nodename))
			  newPossibleMatches.add(currentNode);
	  }
	  curMatches = newPossibleMatches;
  }
  /**
   * This updates the possibleDOMMatches to be the ChildrenNodes
   * so that all the childNodes can be compared to the next step of the XPath query
   */
  private void updatePossibleMatchesWithChildNodes()
  {
	  List<Node> newPossibleMatches = new ArrayList<Node>();
	  for(Node currentNode : curMatches)
	  {
		  NodeList childNodes = currentNode.getChildNodes();
		  for(int i=0; i<childNodes.getLength(); i++)
		  {
			  if(childNodes.item(i).getNodeType()==Node.ELEMENT_NODE)
			  {
				  newPossibleMatches.add(childNodes.item(i));
				  //System.out.println("child nodes: "+childNodes.item(i).getNodeName());
			  }
		  }
	  }
	  curMatches = newPossibleMatches;
  }
  /**
   * This updates the possibleMatches for a condition recursive path to be the valid ChildrenNodes
   * so that all the childNodes can be compared to the next step of the condition or so they can be used
   * to remove the DOMMatches that are no longer valid based on the condition of the step
   */
  private List<Node> updateConditionMatchesWithMatchingChildNodes(String comparisonNodeName, List<Node> possibleConditionMatches)
  {
	  ArrayList<Node> newPossibleMatches = new ArrayList<Node>();
	  for(Node currentNode : possibleConditionMatches)
	  {
		  NodeList childNodes = currentNode.getChildNodes();
		  for(int i=0; i<childNodes.getLength(); i++)
		  {
			  if(childNodes.item(i).getNodeType()==Node.ELEMENT_NODE)
			  {
				  if(childNodes.item(i).getNodeName().equals(comparisonNodeName))
					  newPossibleMatches.add(childNodes.item(i));
			  }
		  }
	  }
	  return newPossibleMatches;
  }
  /**
   * handles parsing the DOM possible matches for the condition/test:contains(text(), "")
   * @param quoteText
   * @param possibleConditionMatches
   * @return
   */
  private List<Node> updateConditionMatchesWithContainsText(String quoteText, List<Node> possibleConditionMatches)
  {
	  ArrayList<Node> newPossibleMatches = new ArrayList<Node>();
	  for(Node currentNode : possibleConditionMatches)
	  {
		  NodeList childNodes = currentNode.getChildNodes();
		  for(int i = 0; i<childNodes.getLength();i++)
		  {
			  //if one of the current matches has a child node that is a text node that contains the quote text
			  //then see if the current node has already been added to the newPossibleMatches list and if not, add it
			  if(childNodes.item(i).getNodeType()==Node.TEXT_NODE)
			  {
				  //System.out.println("update Condition matches with Contains text, text node value = "+childNodes.item(i).getNodeValue());
				  String textNodeValue = fixTextNodeEscapedQuotes(childNodes.item(i).getNodeValue());
				  if(textNodeValue.contains(quoteText))
				  {
					  //System.out.println("contains text matched, text matched is: "+childNodes.item(i).getNodeValue());
					  if(!newPossibleMatches.contains(currentNode))
						  newPossibleMatches.add(currentNode);
				  }
					  
			  }
		  }
	  }
	  return newPossibleMatches;
  }
  /**
   * replaces escaped quote characters in xml with a quote character
   * @param text
   * @returns string with &quot; replaced with a quote
   */
  private String fixTextNodeEscapedQuotes(String text)
  {
	  text.replace("&quot;", "\"");
	  return text;
  }
  /**
   * compares the given string to the text node children of all nodes in the arraylist passed in
   * if the string exactly matches the text in the text node, it is added as a possible match and the array is returned
   * @param quoteText
   * @param possibleConditionMatches
   * @returns the new list of possible matches
   */
  private ArrayList<Node> updateConditionMatchesWithExactText(String quoteText, List<Node> possibleConditionMatches)
  {
	  ArrayList<Node> newPossibleMatches = new ArrayList<Node>();
	  for(Node currentNode : possibleConditionMatches)
	  {
		  NodeList childNodes = currentNode.getChildNodes();
		  for(int i = 0; i<childNodes.getLength();i++)
		  {
			  //if one of the current matches has a child node that is a text node that contains the quote text
			  //then see if the current node has already been added to the newPossibleMatches list and if not, add it
			  if(childNodes.item(i).getNodeType()==Node.TEXT_NODE)
			  {
				  //System.out.println("update Condition matches with exact text, text node value = "+childNodes.item(i).getNodeValue());
				  String textNodeValue = fixTextNodeEscapedQuotes(childNodes.item(i).getNodeValue());
				  if(textNodeValue.equals(quoteText))
				  {
					  //System.out.println("exact text matched, text matched is: "+childNodes.item(i).getNodeValue());
					  if(!newPossibleMatches.contains(currentNode))
						  newPossibleMatches.add(currentNode);
				  }
					  
			  }
		  }
	  }
	  return newPossibleMatches;
  }
  /**
   * This method checks that the Nodes in the arraylist of possible matches have an attribute 
   * with the specified attributeName and attributeText.If they do, they are added to the new 
   * possible matches arraylist and returned.
   * @param attributeName
   * @param attributeText
   * @param possibleConditionMatches
   * @return
   */
  private ArrayList<Node>updateConditionMatchesWithAttribute(String attributeName, String attributeText, List<Node> possibleConditionMatches)
  {
	  ArrayList<Node> newPossibleMatches = new ArrayList<Node>();
	  for(Node currentNode : possibleConditionMatches)
	  {
		  NamedNodeMap allCurrentAttributes = currentNode.getAttributes();
		  if(allCurrentAttributes!=null)
		  {
			  Node selectedAttribute = allCurrentAttributes.getNamedItem(attributeName);
			  if(selectedAttribute!=null&&selectedAttribute.getNodeValue().equals(attributeText))
				  newPossibleMatches.add(currentNode);
		  }
	  }
	  return newPossibleMatches;
  }
  /**
   * This removes Nodes from the first arraylist that are not ancestors or equal to nodes in the second list.
   * It returns the updated arraylist.
   * @param possibleMatchesBeforeCondition
   * @param possibleMatchesAfterCondition
   * @return
   */
  private List<Node> removeNodesNotMatchingIntermediateCondition(List<Node> possibleMatchesBeforeCondition, List<Node> possibleMatchesAfterCondition)
  {
	  ArrayList<Node> newListOfMatches = new ArrayList<Node>();
	  for(Node currentDescendantNode : possibleMatchesAfterCondition)
	  {
		  //System.out.println("current condition matched nodes in removeNodesNotMatchingCondition: "+currentDescendantNode.getNodeName());
		  for(Node currentOriginalNode : possibleMatchesBeforeCondition)
		  {
			  //System.out.println("current original node in condition matched nodes in removeNodesNotMatchingCondition: "+currentOriginalNode.getNodeName());
			  //if the node after matching is the same as it was before the condition, e.g. its not a child of the original,
			  //then add the original to the new list again.
			  if(currentOriginalNode.isEqualNode(currentDescendantNode)&&!newListOfMatches.contains(currentOriginalNode))
				  newListOfMatches.add(currentOriginalNode);
				
			  //System.out.println("compare document position result = "+currentDescendantNode.compareDocumentPosition(currentOriginalNode));
			  //if the original node contains the descendant node then this is a node that should be kept as a possible match
			  if((currentDescendantNode.compareDocumentPosition(currentOriginalNode)== (Node.DOCUMENT_POSITION_CONTAINS+Node.DOCUMENT_POSITION_PRECEDING)))
			  {
				  //System.out.println("document position contains");
				  if(!newListOfMatches.contains(currentOriginalNode))
					  newListOfMatches.add(currentOriginalNode);
			  }
			  else if(currentDescendantNode.compareDocumentPosition(currentOriginalNode)==Node.DOCUMENT_POSITION_CONTAINED_BY)
			  {
				  //System.out.println("document position contained by");
			  }
		  }
	  }
	  return newListOfMatches;
	  
  }
  /**
   * This method removes nodes from the global list if they are not ancestor nodes of the nodes in the passed in ArrayList, 
   * meaning the node did not match the condition and is no longer a possible match.
   * @param matchesAfter
   */
  private void removeAfterCond(List<Node> matchesAfter)
  {
	  ArrayList<Node> newListOfMatches = new ArrayList<Node>();
	  for(Node currentDescendantNode : matchesAfter)
	  {
		  //System.out.println("current condition matched nodes in removeNodesNotMatchingCondition: "+currentDescendantNode.getNodeName());
		  for(Node currentOriginalNode : curMatches)
		  {

			  if(currentOriginalNode.isEqualNode(currentDescendantNode)&&!newListOfMatches.contains(currentOriginalNode))
				  newListOfMatches.add(currentOriginalNode);
				
			  
			  if((currentDescendantNode.compareDocumentPosition(currentOriginalNode)== (Node.DOCUMENT_POSITION_CONTAINS+Node.DOCUMENT_POSITION_PRECEDING)))
			  {
				  //System.out.println("document position contains");
				  if(!newListOfMatches.contains(currentOriginalNode))
					  newListOfMatches.add(currentOriginalNode);
			  }
			  else if(currentDescendantNode.compareDocumentPosition(currentOriginalNode)==Node.DOCUMENT_POSITION_CONTAINED_BY)
			  {
				  //System.out.println("document position contained by");
			  }
		  }
	  }
	  curMatches = newListOfMatches;
  }
@Override
public boolean isSAX() {

	return false;
}
@Override
public boolean[] evaluateSAX(InputStream document, DefaultHandler handler) {
	// TODO Auto-generated method stub
	return null;
}
        
}