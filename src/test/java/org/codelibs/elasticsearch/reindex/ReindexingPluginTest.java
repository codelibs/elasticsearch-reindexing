package org.codelibs.elasticsearch.reindex;

import junit.framework.TestCase;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.Curl;
import org.codelibs.elasticsearch.runner.net.CurlRequest;
import org.codelibs.elasticsearch.runner.net.CurlResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;

import java.util.Map;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

public class ReindexingPluginTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    private String clusterName;

    @Override
    protected void setUp() throws Exception {
        clusterName = "es-reindexing-" + System.currentTimeMillis();
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        /**
         * pass an ElasticsearchClusterRunner.Builder object into runner,
         * and build the cluster
         * Builder is an inner interface, implementing while using
         */
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("http.cors.allow-origin", "*");
                settingsBuilder.put("index.number_of_shards", 3);
                settingsBuilder.put("index.number_of_replicas", 0);
                settingsBuilder.putArray("discovery.zen.ping.unicast.hosts",
                        "localhost:9301-9310");
                settingsBuilder.put("plugin.types",
                        "org.codelibs.elasticsearch.reindex.ReindexingPlugin");
                settingsBuilder
                        .put("index.unassigned.node_left.delayed_timeout", "0");
            }
        }).build(newConfigs().numOfNode(1).clusterName(clusterName));

        // wait for yellow status: all primary shards available, not all replica shards
        runner.ensureYellow();
    }

    @Override
    protected void tearDown() throws Exception {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
    }

    public void test_reindexing() throws Exception {

        final String index = "dataset";
        final String type = "item";

        // create an index
        runner.createIndex(index, (Settings) null);
        if (!runner.indexExists(index)) {
            fail();
        }

        // create 1000 documents
        for (int i = 1; i <= 1000; i++) {
            final IndexResponse indexResponse1 = runner.insert(index, type, String.valueOf(i),
                    "{\"msg\":\"test " + i + "\", \"id\":\"" + i + "\"}");
            assertTrue(indexResponse1.isCreated());
        }
        runner.refresh();

        // search 1000 documents
        final SearchResponse searchResponse = runner.search(index, type, null, null, 0, 10);
        assertEquals(1000, searchResponse.getHits().getTotalHits());

        assertTrue(runner.indexExists(index));

        // return an available node of the cluster
        Node node = runner.node();

        runner.ensureGreen();
        test_index_to_newIndex(node, index, type);

//        runner.ensureGreen();
//        test_index_to_remote_newIndex_withSource(node, index, type);
//
//        runner.ensureGreen();
//        test_index_to_newIndex_withSource(node, index, type);
//
//        runner.ensureGreen();
//        test_index_type_to_newIndex_newType(node, index, type);
//
//        runner.ensureGreen();
//        test_index_type_to_newIndex(node, index, type);
//
//        runner.ensureGreen();
//        test_index_type_to_remote_newIndex_newType(node, index, type);
//
//        runner.ensureGreen();
//        test_index_type_to_remote_newIndex(node, index, type);
//
//        runner.ensureGreen();
//        test_index_to_remote_newIndex(node, index, type);
    }

    private void test_index_type_to_newIndex_newType(Node node, String index, String type)
            throws Exception {
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

    /**
     * Reindex all the documents of {@param index}
     *
     * @param node
     * @param index
     * @param type
     * @throws Exception
     */
    private void test_index_to_newIndex(Node node, String index, String type) throws Exception {
        String newIndex = "dataset2";
        String newType = type;

        CurlRequest curlRequest = Curl.post(node, "/" + index + "/_reindex/" + newIndex)
                .param("wait_for_completion", "true");
        CurlResponse curlResponse = curlRequest.execute();

        Map<String, Object> map = curlResponse.getContentAsMap();
        assertTrue(((Boolean) map.get("acknowledged")).booleanValue());
        assertNull(map.get("name"));

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        final SearchResponse searchResponse = runner.search(newIndex, newType, null, null, 0, 10);
        assertEquals(1000, searchResponse.getHits().getTotalHits());

        runner.deleteIndex(newIndex);
    }

    private void test_index_to_newIndex_withSource(Node node, String index, String type)
            throws Exception {
        String newIndex = "dataset2";
        String newType = type;

        try (CurlResponse curlResponse = Curl
                .post(node, "/" + index + "/_reindex/" + newIndex)
                .param("wait_for_completion", "true")
                .body("{\"query\":{\"term\":{\"msg\":{\"value\":\"100\"}}}}")
                .execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(((Boolean) map.get("acknowledged")).booleanValue());
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search 1 documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newType, null, null, 0, 10);
            assertEquals(1, searchResponse.getHits().getTotalHits());
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

    private void test_index_to_remote_newIndex_withSource(Node node, String index, String type) throws Exception {
        String newIndex = "dataset2";
        String newType = type;

        try (CurlResponse curlResponse = Curl
                .post(node, "/" + index + "/_reindex/" + newIndex)
                .param("wait_for_completion", "true")
                .param("url", "http://localhost:" + node.settings().get("http.port"))
                .body("{\"query\":{\"term\":{\"msg\":{\"value\":\"100\"}}}}")
                .execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(((Boolean) map.get("acknowledged")).booleanValue());
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search 1 documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newType, null, null, 0, 10);
            assertEquals(1, searchResponse.getHits().getTotalHits());
        }

        runner.deleteIndex(newIndex);
    }

    public void test_parentChild() throws Exception {

        final String index = "company";
        final String parentType = "branch";
        final String childType = "employee";

        // create an index
        runner.createIndex(index, (Settings) null);
        runner.createMapping(index, childType, "{\"_parent\":{\"type\":\""
                + parentType + "\"}}");

        if (!runner.indexExists(index)) {
            fail();
        }

        // create parent 1000 documents
        for (int i = 1; i <= 100; i++) {
            final IndexResponse indexResponse1 = runner.insert(index,
                    parentType, String.valueOf(i), "{\"name\":\"Branch" + i
                            + "\"}");
            assertTrue(indexResponse1.isCreated());
            for (int j = 1; j <= 10; j++) {
                final IndexResponse indexResponse2 = runner
                        .client()
                        .prepareIndex(index, childType, i + "_" + j)
                        .setSource(
                                "{\"name\":\"Taro " + i + "_" + j
                                        + "\", \"age\":\"" + (i % 20 + 20)
                                        + "\"}").setParent(String.valueOf(i))
                        .setRefresh(true).execute().actionGet();
                assertTrue(indexResponse2.isCreated());
            }
        }
        runner.refresh();

        // search 100 parent documents
        {
            final SearchResponse searchResponse = runner.search(index,
                    parentType, null, null, 0, 10);
            assertEquals(100, searchResponse.getHits().getTotalHits());
        }
        // search 1000 child documents
        {
            final SearchResponse searchResponse = runner.search(index,
                    childType, null, null, 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits());
        }
        // search 5 parent documents
        {
            final SearchResponse searchResponse = runner
                    .search(index, parentType, QueryBuilders.hasChildQuery(
                            childType, QueryBuilders.matchQuery("age", "20")),
                            null, 0, 10);
            assertEquals(5, searchResponse.getHits().getTotalHits());
        }

        Node node = runner.node();

        runner.ensureGreen();
        test_index_to_newIndex_pc(node, index, parentType, childType);

        runner.ensureGreen();
        test_index_type_to_newIndex_pc(node, index, parentType, childType);

        runner.ensureGreen();
        test_index_to_remote_newIndex_pc(node, index, parentType, childType);

        runner.ensureGreen();
        test_index_type_to_remote_newIndex_pc(node, index, parentType,
                childType);
    }

    private void test_index_type_to_remote_newIndex_pc(Node node, String index,
                                                       String parentType, String childType) throws Exception {
        String newIndex = "company2";
        String newParentType = parentType;
        String newChildType = childType;

        // create an index
        runner.createIndex(newIndex, (Settings) null);
        runner.createMapping(newIndex, newChildType,
                "{\"_parent\":{\"type\":\"" + parentType + "\"}}");

        // reindex
        try (CurlResponse curlResponse = Curl
                .post(node,
                        "/" + index + "/" + parentType + "," + childType
                                + "/_reindex/" + newIndex + "/")
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

        // search 100 parent documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newParentType, null, null, 0, 10);
            assertEquals(100, searchResponse.getHits().getTotalHits());
        }
        // search 1000 child documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newChildType, null, null, 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits());
        }
        // search 5 parent documents
        {
            final SearchResponse searchResponse = runner
                    .search(newIndex, newParentType, QueryBuilders
                                    .hasChildQuery(newChildType,
                                            QueryBuilders.matchQuery("age", "20")),
                            null, 0, 10);
            assertEquals(5, searchResponse.getHits().getTotalHits());
        }
        runner.deleteIndex(newIndex);
    }

    private void test_index_to_remote_newIndex_pc(Node node, String index,
                                                  String parentType, String childType) throws Exception {
        String newIndex = "company2";
        String newParentType = parentType;
        String newChildType = childType;

        // create an index
        runner.createIndex(newIndex, (Settings) null);
        runner.createMapping(newIndex, newChildType,
                "{\"_parent\":{\"type\":\"" + parentType + "\"}}");

        // reindex
        try (CurlResponse curlResponse = Curl
                .post(node, "/" + index + "/_reindex/" + newIndex + "/")
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

        // search 100 parent documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newParentType, null, null, 0, 10);
            assertEquals(100, searchResponse.getHits().getTotalHits());
        }
        // search 1000 child documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newChildType, null, null, 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits());
        }
        // search 5 parent documents
        {
            final SearchResponse searchResponse = runner
                    .search(newIndex, newParentType, QueryBuilders
                                    .hasChildQuery(newChildType,
                                            QueryBuilders.matchQuery("age", "20")),
                            null, 0, 10);
            assertEquals(5, searchResponse.getHits().getTotalHits());
        }
        runner.deleteIndex(newIndex);
    }

    private void test_index_type_to_newIndex_pc(Node node, String index,
                                                String parentType, String childType) throws Exception {
        String newIndex = "company2";
        String newParentType = parentType;
        String newChildType = childType;

        // create an index
        runner.createIndex(newIndex, (Settings) null);
        runner.createMapping(newIndex, newChildType,
                "{\"_parent\":{\"type\":\"" + parentType + "\"}}");

        // reindex
        try (CurlResponse curlResponse = Curl
                .post(node,
                        "/" + index + "/" + parentType + "," + childType
                                + "/_reindex/" + newIndex + "/")
                .param("wait_for_completion", "true").execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(((Boolean) map.get("acknowledged")).booleanValue());
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search 100 parent documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newParentType, null, null, 0, 10);
            assertEquals(100, searchResponse.getHits().getTotalHits());
        }
        // search 1000 child documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newChildType, null, null, 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits());
        }
        // search 5 parent documents
        {
            final SearchResponse searchResponse = runner
                    .search(newIndex, newParentType, QueryBuilders
                                    .hasChildQuery(newChildType,
                                            QueryBuilders.matchQuery("age", "20")),
                            null, 0, 10);
            assertEquals(5, searchResponse.getHits().getTotalHits());
        }
        runner.deleteIndex(newIndex);
    }

    private void test_index_to_newIndex_pc(Node node, String index,
                                           String parentType, String childType) throws Exception {
        String newIndex = "company2";
        String newParentType = parentType;
        String newChildType = childType;

        // create an index
        runner.createIndex(newIndex, (Settings) null);
        runner.createMapping(newIndex, newChildType,
                "{\"_parent\":{\"type\":\"" + parentType + "\"}}");

        // reindex
        try (CurlResponse curlResponse = Curl
                .post(node, "/" + index + "/_reindex/" + newIndex + "/")
                .param("wait_for_completion", "true").execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(((Boolean) map.get("acknowledged")).booleanValue());
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search 100 parent documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newParentType, null, null, 0, 10);
            assertEquals(100, searchResponse.getHits().getTotalHits());
        }
        // search child documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newChildType, null, null, 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits());
        }
        // search a certain parent documents
        {
            final SearchResponse searchResponse = runner
                    .search(newIndex, newParentType, QueryBuilders
                                    .hasChildQuery(newChildType,
                                            QueryBuilders.matchQuery("age", "20")),
                            null, 0, 10);
            assertEquals(5, searchResponse.getHits().getTotalHits());
        }
        runner.deleteIndex(newIndex);
    }
}
