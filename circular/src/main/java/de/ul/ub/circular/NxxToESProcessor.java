package de.ul.ub.circular;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NxxToESProcessor implements Processor {

    private String indexName = "erm";
    private String docType = "spo";
    private int bulkSize = 10000;

    private String clusterName = "circular";
    private String[] hosts = { "localhost" };
    private int port = 9300;

    private TransportClient client;

    private static Logger logger = LoggerFactory
            .getLogger(NxxToESProcessor.class);

    public NxxToESProcessor() {
        final Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", clusterName).build();
        this.client = new TransportClient(settings);
        for (String host : hosts) {
            client.addTransportAddress(new InetSocketTransportAddress(host,
                    port));
        }
        
        
    }

    public String getClusterName() {
        return clusterName;
    }

    public NxxToESProcessor setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public String[] getHosts() {
        return hosts;
    }

    public NxxToESProcessor setHosts(String[] hosts) {
        this.hosts = hosts;
        return this;
    }

    public int getPort() {
        return port;
    }

    public NxxToESProcessor setPort(int port) {
        this.port = port;
        return this;
    }

    public String getIndexName() {
        return indexName;
    }

    public NxxToESProcessor setIndexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public String getDocType() {
        return docType;
    }

    public NxxToESProcessor setDocType(String docType) {
        this.docType = docType;
        return this;
    }

    public int getBulkSize() {
        return bulkSize;
    }

    public NxxToESProcessor setBulkSize(int bulkSize) {
        this.bulkSize = bulkSize;
        return this;
    }

    private void handleBulkFailure(BulkResponse response) {
        logger.error("FAILED, bulk not indexed. exiting now.");
        Iterator<BulkItemResponse> it = response.iterator();
        while (it.hasNext()) {
            BulkItemResponse bir = it.next();
            if (bir.failed()) {
                Failure failure = bir.getFailure();
                logger.error("id: " + failure.getId() + ", message: "
                        + failure.getMessage() + ", type: " + failure.getType()
                        + ", index: " + failure.getIndex());
            }
        }
    }

    public void process(Exchange exchange) throws Exception {

        File file = exchange.getIn().getBody(File.class);

        final BufferedReader in = new BufferedReader(new InputStreamReader(
                new FileInputStream(file), "UTF8"));
        final NxParser nxp = new NxParser(in);

        Node[] nxx;
        Map<String, Object> item = new HashMap<String, Object>();
        long counter = 0;

        logger.info("processing " + file.getAbsolutePath());

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        while (nxp.hasNext()) {
            nxx = (Node[]) nxp.next();

            item.put("s", nxx[0].toString());
            item.put("p", nxx[1].toString());
            item.put("o", nxx[2].toString());

            bulkRequest.add(this.client.prepareIndex(this.indexName,
                    this.docType).setSource(item));
            counter += 1;

            if (counter % this.bulkSize == 0) {
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    handleBulkFailure(bulkResponse);
                } else {
                    logger.info("indexed " + counter + " docs");
                    bulkRequest = client.prepareBulk();
                }
            }
        }

        // final batch
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            handleBulkFailure(bulkResponse);
        } else {
            logger.info("indexed " + counter + " docs");
            bulkRequest = client.prepareBulk();
        }
        logger.info("finished indexing " + file.getAbsolutePath());
    }
}
