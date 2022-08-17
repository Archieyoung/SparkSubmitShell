package org.bgi.flexlab;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import org.apache.commons.cli.*;

public class SparkSubmitShellOptions {
    private static final Logger logger = LoggerFactory.getLogger(SparkSubmitShellOptions.class);

    // argument parser
    private Options options = new Options();
    private CommandLine cmdLine;
    private CommandLineParser parser = new PosixParser();

    // program meta data
    private static final String VERSION = "0.1.0.0";

    public String jobStdOut = null;
    public String jobStdErr = null;

    public SparkSubmitShellOptions() {
        addOption("o", "stdout", true, "standard output for the job", false, "FILE");
        addOption("e", "stderr", true, "standard error for the job", false, "FILE");
        addOption("h", "help", false, "print this message and exit.", false, null);
        addOption("V", "version", false, "show version.", false, null);
    }

    public void parse(String[] args) {

        HelpFormatter helpFormatter = new HelpFormatter();

        helpFormatter.setSyntaxPrefix("spark-submit-shell: submit shell job using spark-submit\nVersion: "+VERSION+"\n\n");
        helpFormatter.setWidth(HelpFormatter.DEFAULT_WIDTH*2);
        helpFormatter.setOptionComparator(null);

        if (args.length == 0) {
            helpFormatter.printHelp("Options: ", options);
            System.exit(0);
        }

        // check help and version
        for (String s : args) {
            if (s.equals("-h") || s.equals("--help")) {
                helpFormatter.printHelp("Options: ", options);
                System.exit(0);
            }
            if (s.equals("-V") || s.equals("--version")) {
                System.out.println("version: " + VERSION);
                System.exit(0);
            }
        }

        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            logger.error("{}\n", e.getMessage());
            helpFormatter.printHelp("Options:", options);
            System.exit(1);
        }

        if (args.length == 0 || getOptionFlagValue("h")) {
            helpFormatter.printHelp("Options:", options);
            return;
        }

        if (getOptionFlagValue("V")) {
            System.out.println("Version: " + VERSION);
            return;
        }

        this.jobStdOut = getAbsolutePathFromStr(getOptionValue("o", this.jobStdOut));
        this.jobStdErr = getAbsolutePathFromStr(getOptionValue("e", this.jobStdErr));
    }

    private String getAbsolutePathFromStr(final String s) {
        if (s == null) return null;
        File file = new File(s);
        return file.getAbsolutePath();
    }

    private void addOption(final String opt, final String longOpt, final boolean hasArg, final String description, final boolean isRequired, final String argName) {
        Option option = new Option(opt, longOpt, hasArg, description);
        option.setRequired(isRequired);
        if (hasArg) {
            option.setArgName(argName);
        }
        options.addOption(option);
    }

    protected String[] getOptionValues(String opt, String[] defaultValue) {
        if (cmdLine.hasOption(opt)) 
            return cmdLine.getOptionValues(opt);
        return defaultValue;
    }

    protected String getOptionValue(String opt, String defaultValue) {
		if (cmdLine.hasOption(opt))
			return cmdLine.getOptionValue(opt);
		return defaultValue;
	}

	protected int getOptionIntValue(String opt, int defaultValue) {
		if (cmdLine.hasOption(opt))
			return Integer.parseInt(cmdLine.getOptionValue(opt));
		return defaultValue;
	}

    protected boolean getOptionFlagValue(String opt) {
        if (cmdLine.hasOption(opt))
			return true;
		return false;
    }

	protected boolean getOptionBooleanValue(String opt, boolean defaultValue) {
		if (cmdLine.hasOption(opt)) {
            if (cmdLine.getOptionValue(opt).equals("true")) {
                return true;
            } else if (cmdLine.getOptionValue(opt).equals("false")) {
                return false;
            }
        }
        return defaultValue;
	}

	protected double getOptionDoubleValue(String opt, double defaultValue) {
		if (cmdLine.hasOption(opt))
			return Double.parseDouble(cmdLine.getOptionValue(opt));
		return defaultValue;
	}

	protected long getOptionLongValue(String opt, long defaultValue) {
		if (cmdLine.hasOption(opt))
			return Long.parseLong(cmdLine.getOptionValue(opt));
		return defaultValue;
	}

	protected byte getOptionByteValue(String opt, byte defaultValue) {
		if (cmdLine.hasOption(opt))
			return Byte.parseByte(cmdLine.getOptionValue(opt));
		return defaultValue;
	}

	protected short getOptionShortValue(String opt, short defaultValue) {
		if (cmdLine.hasOption(opt))
			return Short.parseShort(cmdLine.getOptionValue(opt));
		return defaultValue;
	}

    protected String[] getPositionalArgs() {
        return cmdLine.getArgs();
    }
}
