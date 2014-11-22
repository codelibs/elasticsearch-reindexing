package org.codelibs.elasticsearch.reindex;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.util.Map;

import junit.framework.TestCase;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.Curl;
import org.codelibs.elasticsearch.runner.net.CurlResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.node.Node;

public class ReindexingPluginTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    @Override
    protected void setUp() throws Exception {
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("index.number_of_replicas", 0);
            }
        }).build(newConfigs().ramIndexStore().numOfNode(1));

        // wait for yellow status
        runner.ensureYellow();
    }

    @Override
    protected void tearDown() throws Exception {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
    }

    public void test_runCluster() throws Exception {

        final String index = "dataset";
        final String type = "item";

        // create an index
        runner.createIndex(index, null);

        if (!runner.indexExists(index)) {
            fail();
        }

        // create 1000 documents
        for (int i = 1; i <= 1000; i++) {
            final IndexResponse indexResponse1 = runner.insert(index, type,
                    String.valueOf(i), "{\"msg\":\"test " + i + "\", \"id\":\""
                            + i + "\"}");
            assertTrue(indexResponse1.isCreated());
        }
        runner.refresh();

        // search 1000 documents
        {
            final SearchResponse searchResponse = runner.search(index, type,
                    null, null, 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits());
        }

        assertTrue(runner.indexExists(index));

        Node node = runner.node();

        runner.ensureGreen();
        test_index_type_to_newIndex_newType(node, index, type);

        runner.ensureGreen();
        test_index_type_to_newIndex(node, index, type);

        runner.ensureGreen();
        test_index_to_newIndex(node, index, type);

        runner.ensureGreen();
        test_index_type_to_remote_newIndex_newType(node, index, type);

        runner.ensureGreen();
        test_index_type_to_remote_newIndex(node, index, type);

        runner.ensureGreen();
        test_index_to_remote_newIndex(node, index, type);
    }

    private void test_index_type_to_newIndex_newType(Node node, String index,
            String type) throws Exception {
        String newIndex = "dataset2";
        String newType = "item2";

        try (CurlResponse curlResponse = Curl
                .post(node,
                        "/" + index + "/" + type + "/_reindex/" + newIndex
                                + "/" + newType)
                .param("wait_for_completion", "true").execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(((Boolean) map.get("acknowledged")).booleanValue());
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search 1000 documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newType, null, null, 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits());
        }

        runner.deleteIndex(newIndex);
    }

    private void test_index_type_to_newIndex(Node node, String index,
            String type) throws Exception {
        String newIndex = "dataset2";
        String newType = type;

        try (CurlResponse curlResponse = Curl
                .post(node, "/" + index + "/" + type + "/_reindex/" + newIndex)
                .param("wait_for_completion", "true").execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(((Boolean) map.get("acknowledged")).booleanValue());
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search 1000 documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newType, null, null, 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits());
        }

        runner.deleteIndex(newIndex);
    }

    private void test_index_to_newIndex(Node node, String index, String type)
            throws Exception {
        String newIndex = "dataset2";
        String newType = type;

        try (CurlResponse curlResponse = Curl
                .post(node, "/" + index + "/_reindex/" + newIndex)
                .param("wait_for_completion", "true").execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(((Boolean) map.get("acknowledged")).booleanValue());
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search 1000 documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newType, null, null, 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits());
        }

        runner.deleteIndex(newIndex);
    }

    private void test_index_type_to_remote_newIndex_newType(Node node,
            String index, String type) throws Exception {
        String newIndex = "dataset2";
        String newType = "item2";

        try (CurlResponse curlResponse = Curl
                .post(node,
                        "/" + index + "/" + type + "/_reindex/" + newIndex
                                + "/" + newType)
                .param("wait_for_completion", "true")
                .param("url",
                        "http://localhost:" + node.settings().get("http.port"))
                .execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(((Boolean) map.get("acknowledged")).booleanValue());
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search 1000 documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newType, null, null, 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits());
        }

        runner.deleteIndex(newIndex);
    }

    private void test_index_type_to_remote_newIndex(Node node, String index,
            String type) throws Exception {
        String newIndex = "dataset2";
        String newType = type;

        try (CurlResponse curlResponse = Curl
                .post(node, "/" + index + "/" + type + "/_reindex/" + newIndex)
                .param("wait_for_completion", "true")
                .param("url",
                        "http://localhost:" + node.settings().get("http.port"))
                .execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(((Boolean) map.get("acknowledged")).booleanValue());
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search 1000 documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newType, null, null, 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits());
        }

        runner.deleteIndex(newIndex);
    }

    private void test_index_to_remote_newIndex(Node node, String index,
            String type) throws Exception {
        String newIndex = "dataset2";
        String newType = type;

        try (CurlResponse curlResponse = Curl
                .post(node, "/" + index + "/_reindex/" + newIndex)
                .param("wait_for_completion", "true")
                .param("url",
                        "http://localhost:" + node.settings().get("http.port"))
                .execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(((Boolean) map.get("acknowledged")).booleanValue());
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search 1000 documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newType, null, null, 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits());
        }

        runner.deleteIndex(newIndex);
    }
}
