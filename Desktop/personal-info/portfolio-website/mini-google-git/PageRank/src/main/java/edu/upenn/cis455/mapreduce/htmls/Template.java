package edu.upenn.cis455.mapreduce.htmls;

public class Template {

    StringBuilder sb;
    MyElement rootElement;

    public Template() {
        sb = new StringBuilder();
        appendHeader();
        rootElement = new MyElement("div", "container", "");
    }

    public void appendHeader() {
        sb.append("<!DOCTYPE html>\n" +
                "<html>\n" +
                "<head>\n" +
                "    <title>Storm - MapReducer</title>\n" +
                "    <link rel=\"stylesheet\" href=\"https://cdnjs.cloudflare.com/ajax/libs/materialize/1.0.0/css/materialize.min.css\">\n" +
                "    <script src=\"https://cdnjs.cloudflare.com/ajax/libs/materialize/1.0.0/js/materialize.min.js\"></script>\n" +
                "</head>\n" +
                "<body style=\"color:black;\">\n");
    }

    public void insertElement(MyElement element) {
        rootElement.insertElement(element);
    }

    public void appendEnd() {
        sb.append("<p>Written By: Bo Lyu (lyubo@) </p></body>\n" + "</html>");
    }

    public String toStr() {
        sb.append(rootElement.toStr());
        appendEnd();
        return sb.toString();
    }
}
