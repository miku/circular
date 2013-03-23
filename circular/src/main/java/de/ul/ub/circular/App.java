package de.ul.ub.circular;

import java.io.File;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.main.Main;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

public class App {

    private Main main;

    // mandatory
    private String filename;
    private String directory;

    // optional arguments
    private int bulkSize = 10000;
    private String clusterName = "circular";
    private String indexName = "erm";
    private String docType = "spo";

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public int getBulkSize() {
        return bulkSize;
    }

    public void setBulkSize(int bulkSize) {
        this.bulkSize = bulkSize;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getDocType() {
        return docType;
    }

    public void setDocType(String docType) {
        this.docType = docType;
    }

    @SuppressWarnings("static-access")
    public static void main(String[] args) throws Exception {

        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("FILE")
                .withLongOpt("input")
                .withDescription("read and index N-triples file into ES")
                .hasArg().create("i"));

        options.addOption(OptionBuilder.withArgName("N")
                .withLongOpt("bulk-size").withDescription("bulk size (10000)")
                .hasArg().create("n"));

        options.addOption(OptionBuilder.withArgName("NAME").withLongOpt("cluster")
                .withDescription("cluster name (circular)").hasArg().create("c"));

        options.addOption(OptionBuilder.withArgName("NAME").withLongOpt("index")
                .withDescription("index name (erm)").hasArg().create("x"));

        options.addOption(OptionBuilder.withArgName("t").withLongOpt("doctype")
                .withDescription("document type (spo)").hasArg().create("t"));

        options.addOption("h", "help", false, "show help");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("trainman", options, true);
            System.exit(0);
        }

        if (cmd.hasOption("i")) {
            File file = new File(cmd.getOptionValue("i"));
            App app = new App();
            app.setFilename(file.getName());
            app.setDirectory(file.getParent());
            app.setBulkSize(Integer.parseInt(cmd.getOptionValue("n", "10000")));
            app.setClusterName(cmd.getOptionValue("c", "circular"));
            app.setIndexName(cmd.getOptionValue("x", "erm"));
            app.setDocType(cmd.getOptionValue("t", "spo"));
            app.boot();
        }
    }

    public void boot() throws Exception {
        main = new Main();
        main.enableHangupSupport();
        final String path = this.directory + "?fileName=" + this.filename;
        main.addRouteBuilder(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("file:" + path + "&noop=true").process(
                        new NxxToESProcessor().setBulkSize(bulkSize)
                                .setClusterName(clusterName)
                                .setIndexName(indexName));
            }
        });
        main.run();
        main.stop();
    }
}
