package edu.upenn.cis455.mapreduce.htmls;

import java.util.ArrayList;

public class MyForm {

    String url;
    String method;
    ArrayList<Field> fields;

    private class Field{
        String name, formName, type, tag, placeHolder;
        public Field(String name, String formName, String type, String tag, String placeHolder) {
            this.name = name; this.formName = formName;
            this.tag = tag; this.type = type;
            this.placeHolder = placeHolder;
        }
        public String toStr() {
            StringBuilder sb = new StringBuilder();
            sb.append(name + " <" + tag);
            sb.append(" type=\"" + type + "\"");
            sb.append(" name=\"" + formName + "\"");
            sb.append(" value=\"" + placeHolder + "\"");
            sb.append("<br/>");
            return sb.toString();
        }
    }

    public MyForm(String url, String method) {
        this.url = url;
        this.method = method;
        this.fields = new ArrayList<>();
    }

    public void addTextInputField(String name, String valueName, String placeHolder) {
        addInputField(name, valueName, "text", placeHolder);
    }

    public void addInputField(String name, String formName, String type, String placeHolder) {
        addField(name, formName, type, "input", placeHolder);

    }

    public void addField(String name, String formName, String type, String tag, String placeHolder) {
        fields.add(new Field(name, formName, type, tag, placeHolder));
    }

    public String toStr() {
        StringBuilder sb = new StringBuilder();
        sb.append("<form action=\"" + url + "\" method=\"" + method + "\" target=\"_blank\">");
        for (Field field : fields) {
            sb.append(field.toStr());
        }
        sb.append("<button class=\"btn \" type=\"submit\" name=\"action\">Submit Job\n</button>");
        sb.append("</form>");
        return sb.toString();
    }
}
