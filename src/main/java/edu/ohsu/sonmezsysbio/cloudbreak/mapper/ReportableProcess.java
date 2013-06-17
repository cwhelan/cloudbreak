package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import org.apache.hadoop.mapred.Reporter;

/**
* Created by IntelliJ IDEA.
* User: cwhelan
* Date: 6/13/13
* Time: 2:59 PM
*/
class ReportableProcess {
    public Process process;
    private Reporter reporter;

    public ReportableProcess(Process exec, Reporter reporter) {
        this.process = exec;
        this.reporter = reporter;
    }

    public int waitForWhileReporting() throws InterruptedException {
        while (true) {
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                throw e;
            }
            try {
                int exitVal = process.exitValue();
                return exitVal;
            } catch (IllegalThreadStateException e) {
                reporter.progress();
            }
        }
    }
}
