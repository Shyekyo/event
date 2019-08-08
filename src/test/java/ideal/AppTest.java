package ideal;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    public static void main(String[] args)throws Exception{
        TransportClient client = getClient();
        IndexResponse response = client.prepareIndex("twitter", "_doc", "1")
                .setSource(getEShelper())
                .get();
        // Index name
        String _index = response.getIndex();
        print(_index);
        // Type name
        String _type = response.getType();
        print(_type);
        // Document ID (generated or not)
        String _id = response.getId();
        print(_id);
        // Version (if it's the first time you index this document, you will get: 1)
        long _version = response.getVersion();
        print(String.valueOf(_version));
        // status has stored current instance statement.
        RestStatus status = response.status();
        print(String.valueOf(status.getStatus()));
        close(client);
        human man = new human("kimchy", new Date(), "trying out Elasticsearch");
        ObjectMapper mapper = new ObjectMapper();
        //mapper.writer();
    }
    private static TransportClient getClient()throws Exception{
        Settings elasticsearch_ = Settings.builder()
                .put("cluster.name", "elasticsearch")
                //.put("client.transport.sniff", true)
                //.put("client.transport.ping_timeout", 5)
                //.put("client.transport.nodes_sampler_interval", "5")
                .build();
        TransportClient client = new PreBuiltTransportClient(elasticsearch_)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
        return client;
    }
    private static void close(TransportClient client){
        client.close();
    }

    private static String getManuallyJson(){
        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        return json;
    }
    private static Map<String,Object> getMapJson(){
         Map<String, Object> json = new HashMap<String, Object>();
            json.put("user","kimchy");
            json.put("postDate",new Date());
            json.put("message","trying out Elasticsearch");
         return json;
    }

    private static XContentBuilder getEShelper()throws Exception{
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("user", "kimchy")
                .field("postDate", new Date())
                .field("message", "trying out Elasticsearch")
                .endObject();
        //String json = Strings.toString(builder);
        //System.out.println(json);
        return builder;
    }
    private static void print(String str){
        System.out.println("Print => "+str);
    }
}
