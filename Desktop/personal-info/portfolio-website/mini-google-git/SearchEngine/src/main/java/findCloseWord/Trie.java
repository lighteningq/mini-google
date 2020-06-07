package findCloseWord;

import java.util.HashMap;
import java.util.ArrayList;

/**
 * The trie data structure that is used to store lexicon words.
 */
public class Trie {

    private TrieNode root;

    /**
     * The Trie constructor.
     */
    public Trie() {
        root = new TrieNode();
    }
 
    /**
     * The method inserts a word into the trie.
     * @param word the word to be inserted
     */
    public void insert(String word) {
        HashMap<Character, TrieNode> children = root.children;
 
        for(int i=0; i<word.length(); i++){
            char c = word.charAt(i);
 
            TrieNode t;
            if(children.containsKey(c)){
                    t = children.get(c);
            }else{
                t = new TrieNode(c);
                children.put(c, t);
            }
 
            children = t.children;
 
            //set leaf node
            if(i==word.length()-1)
                t.isLeaf = true;    
        }
    }
 
    /**
     * The method returns if the word is in the trie.
     * @param word the word to be searched
     * @return true if the word is in the trie, false otherwise
     */
    public boolean search(String word) {
        TrieNode t = searchNode(word);
 
        if(t != null && t.isLeaf) 
            return true;
        else
            return false;
    }

    /**
     * The method returns a list of words (up to numWords) in the trie tree that 
     * starts with the prefix. (This is for the auto-complete feature of the
     * dictionary.)
     * @param prefix the prefix of words
     * @param threshold max number of words returned
     * @return a list of words that starts with the prefix
     */
    // 
    public ArrayList<String> getWordsStartsWith(String prefix, int threshold) {
        ArrayList<String> words = new ArrayList<String>();
        TrieNode t = searchNode(prefix);
        if (t == null) {
            return words;
        }
        searchWordsHelper(words, t, prefix, threshold);
                return words;
    }

    /**
     * Hepler class for getWordsStartsWith. It dfs the words (up to threshold) that
     * starts with the prefix
     * @param words the list of words
     * @param t current TrieNode
     * @param word current word string
     * @param threshold size threshold of words searched
     */
    private void searchWordsHelper(ArrayList<String> words, TrieNode t, String word, int threshold) {
        // limit the size of returned list
        if (words.size() >= threshold) {
            return;
        }

        if (t.isLeaf == true) {
            words.add(word);
        }
        for (char c : t.children.keySet()) {
            searchWordsHelper(words, t.children.get(c), word + c, threshold);
        }
    }

    /**
     * Returns if there is any word in the trie that starts with the given prefix.
     * @param prefix the prefix of words
     * @return true if any word in the trie starts with the prefix, false otherwise
     */
    public boolean startsWith(String prefix) {
        if(searchNode(prefix) == null) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Helper class for getWordsStartsWith and startsWith. It searches the node 
     * that matches the given string.
     * @param str the string to be searched
     * @return the TrieNode that matches the given string
     */
    private TrieNode searchNode(String str){
        HashMap<Character, TrieNode> children = root.children; 
        TrieNode t = null;
        for(int i=0; i<str.length(); i++){
            char c = str.charAt(i);
            if(children.containsKey(c)){
                t = children.get(c);
                children = t.children;
            }else{
                return null;
            }
        }
 
        return t;
    }
    
    /**
     * tester
     * @param args
     */
    public static void main(String args[]) {
    	Trie lexicon = new Trie();

        /** tests insert to build tree*/
        lexicon.insert("brake");
        lexicon.insert("service");

        /** tests search for complete word */
        System.out.println(lexicon.search("brake"));
        System.out.println(lexicon.search("bra"));
        System.out.println(lexicon.search("serve"));

        /** tests startsWith */
        System.out.println(lexicon.startsWith("bra"));
        System.out.println(lexicon.startsWith("sea"));

        /** tests getWordsStartsWith */
        lexicon.insert("bra");
        lexicon.insert("brave");
        System.out.println(lexicon.search("bra"));
        ArrayList<String> words = lexicon.getWordsStartsWith("bra", 10);
        for (String w : words) {
            System.out.println(w);
        }
    	
    }
}