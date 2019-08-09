package ideal;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * Created by zhangxiaofan on 2019/8/8.
 */
public class ES { public static void main(String[] args) throws Exception{
    RestHighLevelClient highLevelClient = getHighLevelClient();
    IndexRequest putdoc = putdoc();
    highLevelClient.index(putdoc, RequestOptions.DEFAULT);
    close(highLevelClient);
}

    private static RestHighLevelClient getHighLevelClient(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"))
        );
        return client;
    }
    private static void close(RestHighLevelClient client){
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static IndexRequest putdoc(){
        IndexRequest request = new IndexRequest("posts");
        request.id("1");
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON);
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");
        return request;
    }


}
