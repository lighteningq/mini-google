package LRUCacheQueries;

/**
 * 
 * @apiNote  Design and implement a data structure for Least Recently Used (LRU) cache
 * @implNote inspired by leetcode LRU https://leetcode.com/problems/lru-cache/
 * @author yashu
 *
 */

import java.util.HashMap;

/**
 * Created by QingxiaoDong on 5/4/17.
 * Least Recently Used Cache.
 * get(key) and put(key, value) are both O(1) operations.
 */
public class LRUCache<T> {

    /**
     * Linked list node.
     * Stores the original result entries and the sorted result entries.
     */
    private class Node<T> {
        private String key;
        private T value;
        Node prev;
        Node next;
        public Node (String key, T value) {
            this.key = key;
            this.value = value;
            prev = null;
            next = null;
        }
    }

    private int capacity;
    private HashMap<String, Node> map;
    private Node head;
    private Node tail;

    /**
     * When the cache reached its capacity, it should invalidate the least recently
     * used item before inserting a new item.
     * @param capacity capacity of the LRU cache
     */
    public LRUCache(int capacity) {
        this.capacity = capacity;
        map = new HashMap<String, Node>();
        head = new Node<T>(null, null);
        tail = new Node<T>(null, null);
        head.next = tail;
        tail.prev = head;
    }

    /**
     * Get the value) if the key exists in the cache.
     * @param key key of the result entries
     * @return value or null
     */
    public T get(String key) {
        if (!map.containsKey(key)) {
            return null;
        }
        Node current = map.get(key);
        remove(current);
        add(current);
        return (T) map.get(key).value;
    }

    /**
     * Set or insert the result entries if the key is not already present. When the cache reached
     * its capacity, it should invalidate the least recently used item before inserting a new item.
     * @param key key of the result entries
     * @param value original result entries
     */
    public void put(String key, T value) {
        if (get(key) != null) {
            Node current = map.get(key);
            current.value = value;
        } else {
            if (map.size() == capacity) {
                Node lfu = head.next;
                map.remove(lfu.key);
                remove(lfu);
            }
            Node newNode = new Node(key, value);
            add(newNode);
            map.put(key, newNode);
        }
    }


    /******************************************
     * Helper functions                       *
     ******************************************/

    private void remove(Node current) {
        current.prev.next = current.next;
        current.next.prev = current.prev;
    }

    private void add(Node current) {
        tail.prev.next = current;
        current.prev = tail.prev;
        current.next = tail;
        tail.prev = current;
    }
}
