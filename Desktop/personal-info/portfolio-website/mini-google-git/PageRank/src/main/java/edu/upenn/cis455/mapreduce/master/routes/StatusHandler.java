package edu.upenn.cis455.mapreduce.master.routes;

import edu.upenn.cis455.mapreduce.htmls.*;
import edu.upenn.cis455.mapreduce.master.MasterAppConfig;
import edu.upenn.cis455.mapreduce.master.WorkerStatusInfo;
import spark.Request;
import spark.Response;
import spark.Route;

import java.util.List;

/*
 * Handle GET /status, show a main page for checking worker status and define jobs.
 */
public class StatusHandler implements Route {

    private MasterAppConfig master;

    public StatusHandler(MasterAppConfig master) {
        this.master = master;
    }

    @Override
    public Object handle(Request req, Response res) {
        Template template = new Template();
        appendTitle("h4", template, "Worker Status");
        addTable(template);
        
        appendTitle("h4", template, "Define Job");
        appendForm(template);
        appendButton(template);
        return template.toStr();
    }

    /*
     * Button for start all workers
     */
    private void appendButton(Template template) {
        MyButton sb = new MyButton("/runjobs", !this.master.getJobReady(), "Run Job");
        template.insertElement(new MyElement(sb.toStr()));
    }
    
    /*
     * Table for worker status.
     */

    private void addTable(Template template) {
        MyTable tb = new MyTable("Ip:Port", "Status", "Job", "KeysRead", "KeysWritten", "Running", "Results" );
        List<WorkerStatusInfo> records = master.getWorkerStatus();
        for (WorkerStatusInfo rec : records) {
            String s = master.getResForWorker(rec.getIp());
            long diff = System.currentTimeMillis() - rec.getLastChecked();
            if( diff  > 30000) {
            	// skip the record that did not response in 30 seconds.
            	continue;
            }
            tb.addRow(rec.getIp(), rec.getStatus(),rec.getJobName(), rec.getKeysRead(), rec.getKeysWritten(), rec.getRunningTime(), s);
        }
        template.insertElement(new MyElement(tb.toStr()));
    }
    
    /*
     * add title.
     */
    private void appendTitle(String tag, Template view, String title) {
        view.insertElement(new MyElement(tag, null, title));
    }

    /*
     * Form to define job.
     */
    private void appendForm(Template view) {
        MyForm form = new MyForm("/submitjob", "post");
        form.addTextInputField("Class Name", "class_name", "edu.upenn.cis455.mapreduce.job.PageRankJob");
        form.addTextInputField("Input Directory", "input_directory_path", "PRInputDir/S3");
        form.addTextInputField("Output Directory", "output_directory_path", "PROutputDir");
        form.addInputField("Number of MapBolts", "num_map_bolts", "number", "1");
        form.addInputField("Number of ReduceBolts", "num_reduce_bolts", "number", "1");
        form.addInputField("Number of Iterations", "num_iterations", "number", "1");
        view.insertElement(new MyElement(form.toStr()));
    }
}
