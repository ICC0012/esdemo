package esdemo;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class App {
    public static void main(String[] args) throws Exception {

        Options options = new Options();
        options.addOption("n", "node-client", false, "Run with a ES Node client");
        options.addOption("c", "cluster-name", true, "Name of the cluster to connect to");
        options.addOption(Option.builder("t").longOpt("transport-addresses").desc("Comma-delimited list of node transport addresses (e.g. 'host1:9300,host2:9300')").hasArgs().valueSeparator(',').build());
        CommandLine cmdLine = new DefaultParser().parse(options, args);

        cmdLine.iterator().forEachRemaining(option -> System.out.printf("%s -> %s\n", option.getOpt(), StringUtils.join(option.getValues(), ',')));

        Node node = null;
        Client client = null;
        if (cmdLine.hasOption("n")) {
            // Using Node Client, join the cluster
            NodeBuilder nodeBuilder =  NodeBuilder.nodeBuilder();
            nodeBuilder.clusterName(cmdLine.getOptionValue("c", "elasticsearch"));
            node = nodeBuilder.node();
            client = node.client();
        } else {
            // Using Transport Client
            // NOTE: this talks the transport protocol, not REST (thus port 9300, not 9200)
            Settings tcSettings = ImmutableSettings.builder().put("cluster.name", cmdLine.getOptionValue("c", "elasticsearch")).build();
            TransportClient transportClient = new TransportClient(tcSettings);
            String[] transportAddressesOptions = cmdLine.getOptionValues("t");
            if (transportAddressesOptions == null || transportAddressesOptions.length == 0) {
                transportAddressesOptions = new String[]{"localhost:9300"};
            }
            Arrays.asList(transportAddressesOptions).stream()
                    .map(ta -> ta.split(":"))
                    .map(ta -> new InetSocketTransportAddress(ta[0], Integer.parseInt(ta[1])))
                    .forEach(ista -> transportClient.addTransportAddress(ista));
            client = transportClient;
        }

        Scanner s = new Scanner(System.in);
        while (true) {
            System.out.println("Enter a doc, key <enter> value <enter>, empty key to finish");
            // Build a document in a map (auto-serialized)
            Map<String, Object> doc = new HashMap<>();
            String key;
            while ((key = s.nextLine().trim()).length() > 0) {
                doc.put(key.trim(), s.nextLine().trim());
            }
            if (doc.size() == 0) {
                System.out.println("Empty object. Finished inserting.");
                break;
            }
            IndexResponse index = client.prepareIndex("index", "type").setSource(doc).execute().actionGet();
            System.out.printf("Indexed! id: %s\n", index.getId());
        }

        while (true) {
            System.out.println("Enter an id to GET");
            String id = s.nextLine().trim();
            if (id.length() == 0) {
                System.out.println("Empty id. Finished GETting.");
                break;
            }
            GetResponse get = client.prepareGet("index", "type", id).execute().actionGet();
            System.out.println(get.getSource());
        }

        while (true) {
            System.out.println("Enter a field <enter> query <enter>. Empty field to end");
            String field = s.nextLine().trim();
            if (field.length() == 0) {
                System.out.println("Empty id. Finished GETting.");
                break;
            }
            String queryText = s.nextLine().trim();
            SearchResponse search = client.prepareSearch("index").setTypes("type").setQuery(QueryBuilders.boolQuery().must(
                            QueryBuilders.matchQuery(field, queryText)).mustNot(QueryBuilders.matchPhraseQuery(field, "mechanic engineering"))
            ).execute().actionGet();
            System.out.printf("Got %d hits\n", search.getHits().getTotalHits());
            for (SearchHit searchHit : search.getHits()) {
                System.out.printf("Hit: score %f\n", searchHit.getScore());
                System.out.println(searchHit.getSource());
            }
        }


        client.close();
        if (node != null) {
            node.close();
        }
    }
}
