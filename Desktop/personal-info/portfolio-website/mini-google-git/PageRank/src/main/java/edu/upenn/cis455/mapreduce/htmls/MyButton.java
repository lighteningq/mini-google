package edu.upenn.cis455.mapreduce.htmls;

public class MyButton {

    private String link;
    private boolean disabled;
    private String content;

    public MyButton(String link, boolean disabled, String content) {
        this.link = link; this.disabled = disabled; this.content = content;
    }

    public String toStr() {
        StringBuilder sb = new StringBuilder();
        sb.append("<br>");
        sb.append("<a class=\"btn");
        if (disabled) {
            sb.append(" disabled\"");
        } else {
            sb.append("\"");
        }
        sb.append(" href=\"" + link + "\">");
        sb.append(content);
        sb.append("</a>");
        return sb.toString();
    }

}
