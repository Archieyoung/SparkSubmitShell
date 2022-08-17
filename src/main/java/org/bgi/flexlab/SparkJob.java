package org.bgi.flexlab;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.Runtime;
import java.util.Iterator;
import java.util.ArrayList;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class SparkJob implements Function2<Integer, Iterator<Integer>, Iterator<Integer>> {
    private static final Logger logger = LoggerFactory.getLogger(SparkJob.class);
    public String shell = null;
    public String shell_args = null;
    public String jobStdOut = null;
    public String jobStdErr = null;

    public SparkJob(final String shell, final String shell_args, final String jobStdOut, final String jobStdErr) {
        this.shell = shell;
        this.shell_args = shell_args;
        this.jobStdOut = jobStdOut;
        this.jobStdErr = jobStdErr;
    }

    @Override
    public Iterator<Integer> call(Integer idx, Iterator<Integer> jobIdIter) {
        ArrayList<Integer> result = new ArrayList<>();
        if (jobIdIter.hasNext()) {
            String cmd = String.format("bash %s %s", shell, shell_args);
            logger.info("cmd: {} {}", shell, shell_args);
            try {
                Process proc = Runtime.getRuntime().exec(cmd);
                BufferedReader procStdOut = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                BufferedReader procStdErr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));


                BufferedWriter jobStdOutWriter = null;
                BufferedWriter jobStdErrWriter = null;

                if (jobStdOut != null) {
                    jobStdOutWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(jobStdOut)));
                }

                if (jobStdErr != null) {
                    jobStdErrWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(jobStdErr)));
                }

                String line = null;
                while ((line = procStdOut.readLine()) != null) {
                    if (jobStdOutWriter != null) {
                        jobStdOutWriter.write(line);
                        jobStdOutWriter.newLine();
                    } else {
                        System.out.println(line);
                    }
                }

                while ((line = procStdErr.readLine()) != null) {
                    if (jobStdErrWriter != null) {
                        jobStdErrWriter.write(line);
                        jobStdErrWriter.newLine();
                    } else {
                        System.err.println(line);
                    }
                }

                if (jobStdErrWriter != null) jobStdErrWriter.close();
                if (jobStdOutWriter != null) jobStdOutWriter.close();
                procStdOut.close();
                procStdErr.close();

                result.add(proc.waitFor());
                proc.destroy();
            } catch (Exception e) {
                logger.error("Failed to execute {}, error message: {}", cmd, e.getMessage());
                System.exit(1);
            }
        } else {
            result.add(4); // never get here
            return result.iterator();
        }
        return result.iterator();
    }
}
