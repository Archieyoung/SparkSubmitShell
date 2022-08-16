package org.bgi.flexlab;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.Runtime;
import java.util.Iterator;
import java.util.ArrayList;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class SparkJob implements Function2<Integer, Iterator<Integer>, Iterator<Integer>> {
    private static final Logger logger = LoggerFactory.getLogger(SparkJob.class);
    public String shell;
    public String shell_args;

    public SparkJob(final String shell, final String shell_args) {
        this.shell = new File(shell).getAbsolutePath();
        this.shell_args = shell_args;
    }

    @Override
    public Iterator<Integer> call(Integer idx, Iterator<Integer> jobIdIter) {
        ArrayList<Integer> result = new ArrayList<>();
        if (jobIdIter.hasNext()) {
            final int workIdx = jobIdIter.next();
            String cmd = String.format("sh %s %s", shell, shell_args);
            logger.info("cmd: {} {}", shell, shell_args);
            try {
                Process proc = Runtime.getRuntime().exec(cmd);
                result.add(proc.waitFor());
                BufferedReader procStdOut = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                BufferedReader procStdErr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
                String line = null;
                while ((line = procStdOut.readLine()) != null) {
                    logger.info("cmd stdout: {}", workIdx, line);
                }

                while ((line = procStdErr.readLine()) != null) {
                    logger.info("cmd stderr: {}", workIdx, line);
                }

                procStdOut.close();
                procStdErr.close();

                proc.destroy();
            } catch (Exception e) {
                logger.error("Failed to execute {}, error message: ", cmd, e.getMessage());
                System.exit(1);
            }
        } else {
            result.add(4); // never get here
            return result.iterator();
        }
        return result.iterator();
    }
}
