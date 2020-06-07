package edu.upenn.cis455.mapreduce.htmls;

import java.util.ArrayList;

public class MyElement {


    private String content;
    private String tag;
    private String className;
    private ArrayList<MyElement> innerElements;

    boolean stringOnly;

    public MyElement(String str) {
        this.content = str;
        stringOnly = true;
    }

    public MyElement(String tag, String className, String content) {
        this.tag = tag;
        this.stringOnly = false;
        this.className = className;
        this.innerElements = new ArrayList<>();
        innerElements.add(new MyElement(content));
    }

    public MyElement(String tag, String className, MyElement... elements) {
        this.tag = tag;
        this.stringOnly = false;
        this.className = className;
        this.innerElements = new ArrayList<>();
        for (MyElement element : elements) {
            innerElements.add(element);
        }
    }

    public void insertElement(MyElement element) {
        this.innerElements.add(element);
    }


    public String toStr() {
        if (stringOnly) { return this.content; }
        StringBuilder sb = new StringBuilder();
        sb.append("<" + tag);
        if (className != null) {
            sb.append(" class=" + className);
        }
        sb.append(">");
        for (MyElement e : innerElements) {
            sb.append(e.toStr());
        }
        sb.append("</" + tag + ">\n");
        return sb.toString();
    }
}
