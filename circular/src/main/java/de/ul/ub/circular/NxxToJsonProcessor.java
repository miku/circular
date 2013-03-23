package de.ul.ub.circular;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NxxToJsonProcessor implements Processor {

    private static Logger logger = LoggerFactory.getLogger(App.class);

    public void process(Exchange exchange) throws Exception {
        File file = exchange.getIn().getBody(File.class);
        logger.info("processing: " + file.getAbsolutePath());
        final BufferedReader in = new BufferedReader(new InputStreamReader(
                new FileInputStream(file), "UTF8"));
        final NxParser nxp = new NxParser(in);
        Node[] nxx;
        Map<String, String> item = new HashMap<String, String>();
        while (nxp.hasNext()) {
            nxx = (Node[]) nxp.next();
            item.put("s", nxx[0].toString());
            item.put("p", nxx[1].toString());
            item.put("o", nxx[2].toString());
            logger.info("" + item);
            exchange.getIn().setBody(item);
        }
    }
}
