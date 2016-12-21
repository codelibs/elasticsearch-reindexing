package org.codelibs.elasticsearch.reindex;

import junit.framework.TestCase;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.BuilderCallback;
import org.codelibs.elasticsearch.runner.net.Curl;
import org.codelibs.elasticsearch.runner.net.CurlResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.util.Map;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

public class ReindexingPluginTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    private String clusterName;

    private final int docNumber = 25;
    private final int parentNumber = 10;
    private final int childNumber = 5;

    @Override
    protected void setUp() throws Exception {
        clusterName = "es-reindexing-" + System.currentTimeMillis();
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
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

    public void test_reindexing() throws Exception {

        final String index = "dataset";
        final String type = "item";

        create_index(runner, index, type, docNumber);

        Node node = runner.node();

        runner.ensureGreen();
        test_wait_for_completion(node, index);

        runner.ensureGreen();
        test_index_to_newIndex(node, index);

        runner.ensureGreen();
        test_index_to_newIndex_withSource(node, index);

        runner.ensureGreen();
        test_index_type_to_newIndex(node, index, type);

        runner.ensureGreen();
        test_index_type_to_newIndex_newType(node, index, type);

        runner.ensureGreen();
        test_index_to_remote_newIndex(node, index);

        runner.ensureGreen();
        test_index_to_remote_newIndex_withSource(node, index);

        runner.ensureGreen();
        test_index_type_to_remote_newIndex(node, index, type);

        runner.ensureGreen();
        test_index_type_to_remote_newIndex_newType(node, index, type);

        runner.ensureGreen();
        test_reindex_with_deletion(node, index);

        create_index(runner, index, type, docNumber);

        runner.ensureGreen();
        test_reindex_with_deletion(node, index, type);
    }

    private void create_index(ElasticsearchClusterRunner runner, String index, String type, int number) {

        if (runner.indexExists(index))
            runner.deleteIndex(index);

        runner.createIndex(index, (Settings) null);

        if (!runner.indexExists(index))
            fail();

        for (int i = 0; i < number; i++) {
            final IndexResponse response = runner.insert(index, type, String.valueOf(i),
                    "{\"msg\":\"test " + i + "\", \"id\":\"" + i + "\"}");
            assertTrue(response.isCreated());
        }

        // make it searchable immediately
        runner.refresh();

        // search documents
        final SearchResponse searchResponse = runner.search(index, type, null, null, 0, 10);
        assertEquals(number, searchResponse.getHits().getTotalHits());

        assertTrue(runner.indexExists(index));
    }

    private void test_wait_for_completion(Node node, String index) {
        String newIndex0 = "dataset0", newIndex1 = "dataset1";

        CurlResponse response0 = Curl.post(node, "/" + index + "/_reindex/" + newIndex0)
                .param("wait_for_completion", "true").execute();
        CurlResponse response1 = Curl.post(node, "/" + index + "/_reindex/" + newIndex1)
                .execute();
        Map<String, Object> map0 = response0.getContentAsMap();
        Map<String, Object> map1 = response1.getContentAsMap();
        assertEquals(map0.size(), 1);
        assertTrue(map0.containsKey("acknowledged"));
        assertEquals(map1.size(), 2);
        assertTrue(map1.containsKey("acknowledged"));
        assertTrue(map1.containsKey("name"));

        runner.flush();
        runner.deleteIndex(newIndex0);
        runner.deleteIndex(newIndex1);
    }

    private void test_index_to_newIndex(Node node, String index) throws IOException {
        String newIndex = "dataset2";

        try (CurlResponse curlResponse = Curl
                .post(node, "/" + index + "/_reindex/" + newIndex)
                .param("wait_for_completion", "true")
                .execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(map.containsKey("acknowledged"));
            assertNull(map.get("name"));
        }

        // causes a lucene commit, more expensive than refresh
        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    // BuilderCallback对象用于加工SearchRequestBuilder对象
                    new BuilderCallback<SearchRequestBuilder>() {
                        @Override
                        public SearchRequestBuilder apply(SearchRequestBuilder builder) {
                            return builder;
                        }
                    });
            assertEquals(docNumber, searchResponse.getHits().getTotalHits());
        }

        runner.deleteIndex(newIndex);
    }

    private void test_index_to_newIndex_withSource(Node node, String index) throws IOException {
        String newIndex = "dataset2";

        try (CurlResponse curlResponse = Curl
                .post(node, "/" + index + "/_reindex/" + newIndex)
                .param("wait_for_completion", "true")
                .body("{\"query\":{\"term\":{\"msg\":{\"value\":\"1\"}}}}")
                .execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(map.containsKey("acknowledged"));
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search newly created index
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    new BuilderCallback<SearchRequestBuilder>() {
                        @Override
                        public SearchRequestBuilder apply(SearchRequestBuilder builder) {
                            return builder;
                        }
                    });
            assertEquals(1, searchResponse.getHits().getTotalHits());
        }

        runner.deleteIndex(newIndex);
    }

    private void test_index_type_to_newIndex(Node node, String index, String type) throws IOException {
        String newIndex = "dataset2";
        String newType = type;

        try (CurlResponse curlResponse = Curl
                .post(node, "/" + index + "/" + type + "/_reindex/" + newIndex)
                .param("wait_for_completion", "true")
                .execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(map.containsKey("acknowledged"));
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newType, null, null, 0, 10);
            assertEquals(docNumber, searchResponse.getHits().getTotalHits());
        }

        runner.deleteIndex(newIndex);
    }

    private void test_index_type_to_newIndex_newType(Node node, String index, String type) throws IOException {
        String newIndex = "dataset2";
        String newType = "item2";

        try (CurlResponse curlResponse = Curl
                .post(node,
                        "/" + index + "/" + type + "/_reindex/" + newIndex
                                + "/" + newType)
                .param("wait_for_completion", "true").execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(map.containsKey("acknowledged"));
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newType, null, null, 0, 10);
            assertEquals(docNumber, searchResponse.getHits().getTotalHits());
        }

        runner.deleteIndex(newIndex);
    }

    private void test_index_to_remote_newIndex(Node node, String index) throws IOException {
        String newIndex = "dataset2";

        try (CurlResponse curlResponse = Curl
                .post(node, "/" + index + "/_reindex/" + newIndex)
                .param("wait_for_completion", "true")
                .param("url", "http://localhost:" + node.settings().get("http.port"))
                .execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(map.containsKey("acknowledged"));
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    new BuilderCallback<SearchRequestBuilder>() {
                        @Override
                        public SearchRequestBuilder apply(SearchRequestBuilder builder) {
                            return builder;
                        }
                    });
            assertEquals(docNumber, searchResponse.getHits().getTotalHits());
        }

        runner.deleteIndex(newIndex);
    }

    private void test_index_to_remote_newIndex_withSource(Node node, String index) throws IOException {
        String newIndex = "dataset2";

        try (CurlResponse curlResponse = Curl
                .post(node, "/" + index + "/_reindex/" + newIndex)
                .param("wait_for_completion", "true")
                .param("url",
                        "http://localhost:" + node.settings().get("http.port"))
                .body("{\"query\":{\"term\":{\"msg\":{\"value\":\"1\"}}}}")
                .execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(map.containsKey("acknowledged"));
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    new BuilderCallback<SearchRequestBuilder>() {
                        @Override
                        public SearchRequestBuilder apply(SearchRequestBuilder builder) {
                            return builder;
                        }
                    });
            assertEquals(1, searchResponse.getHits().getTotalHits());
        }

        runner.deleteIndex(newIndex);
    }

    private void test_index_type_to_remote_newIndex(Node node, String index, String type) throws IOException {
        String newIndex = "dataset2";
        String newType = type;

        try (CurlResponse curlResponse = Curl
                .post(node, "/" + index + "/" + type + "/_reindex/" + newIndex)
                .param("wait_for_completion", "true")
                .param("url",
                        "http://localhost:" + node.settings().get("http.port"))
                .execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(map.containsKey("acknowledged"));
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newType, null, null, 0, 10);
            assertEquals(docNumber, searchResponse.getHits().getTotalHits());
        }

        runner.deleteIndex(newIndex);
    }

    private void test_index_type_to_remote_newIndex_newType(Node node, String index, String type) throws IOException {
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
            assertTrue(map.containsKey("acknowledged"));
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newType, null, null, 0, 10);
            assertEquals(docNumber, searchResponse.getHits().getTotalHits());
        }

        runner.deleteIndex(newIndex);
    }

    private void test_reindex_with_deletion(Node node, final String... document_identifier) {

        if (document_identifier.length == 0 || document_identifier.length > 2)
            throw new IllegalArgumentException("args should be index and type[optinal].");

        final String index = document_identifier[0];
        String type = new String();
        if (document_identifier.length == 2) {
            type = "/" + document_identifier[1];
        }

        final String newIndex = "dataset2";

        CurlResponse curlResponse = Curl
                .post(node, "/" + index + type + "/_reindex/" + newIndex)
                .param("wait_for_completion", "true")
                .param("deletion", "true")
                .execute();
        Map<String, Object> map = curlResponse.getContentAsMap();
        assertTrue(map.containsKey("acknowledged"));
        assertNull(map.get("name"));

        runner.flush();

        if (type.isEmpty()) {
            assertFalse(runner.indexExists(index));
        } else {
            assertTrue(runner.indexExists(index));
            final SearchResponse searchResponse = runner.search(index,
                    new BuilderCallback<SearchRequestBuilder>() {
                        @Override
                        public SearchRequestBuilder apply(SearchRequestBuilder builder) {
                            return builder.setTypes(document_identifier[1]);
                        }
                    });
            assertEquals(0, searchResponse.getHits().getTotalHits());
        }
        assertTrue(runner.indexExists(newIndex));

        final SearchResponse searchResponse = runner.search(newIndex,
                new BuilderCallback<SearchRequestBuilder>() {
                    @Override
                    public SearchRequestBuilder apply(SearchRequestBuilder builder) {
                        return builder;
                    }
                });
        assertEquals(docNumber, searchResponse.getHits().getTotalHits());

        runner.deleteIndex(newIndex);
    }

    public void test_parentChild() throws Exception {

        final String index = "company";
        final String parentType = "branch";
        final String childType = "employee";

        final int age = (int) (Math.random() % childNumber) + 1;
        final String ageStr = String.valueOf(age);

        // create an index with parent mapping
        runner.createIndex(index, (Settings) null);
        runner.createMapping(index, childType, "{\"_parent\":{\"type\":\""
                + parentType + "\"}}");

        if (!runner.indexExists(index)) {
            fail();
        }

        // create documents
        for (int i = 1; i <= parentNumber; i++) {
            final IndexResponse indexResponse1 = runner.insert(index, parentType, String.valueOf(i), "{\"name\":\"Branch" + i + "\"}");
            assertTrue(indexResponse1.isCreated());
            for (int j = 1; j <= childNumber; j++) {
                final IndexResponse indexResponse2 = runner
                        .client()
                        .prepareIndex(index, childType, i + "_" + j)
                        .setSource("{\"name\":\"Taro " + i + "_" + j + "\", \"age\":\"" + (j) + "\"}")
                        .setParent(String.valueOf(i))
                        .setRefresh(true).execute().actionGet();
                assertTrue(indexResponse2.isCreated());
            }
        }
        runner.refresh();

        // search parent type documents
        {
            final SearchResponse searchResponse = runner.search(index,
                    parentType, null, null, 0, 10);
            assertEquals(parentNumber, searchResponse.getHits().getTotalHits());
        }
        // search all documents
        {
            final SearchResponse searchResponse = runner.search(index,
                    childType, null, null, 0, 10);
            assertEquals(parentNumber * childNumber, searchResponse.getHits().getTotalHits());
        }
        // search a certain parent documents
        {
            final SearchResponse searchResponse = runner
                    .search(index, parentType,
                            QueryBuilders.hasChildQuery(childType, QueryBuilders.matchQuery("age", ageStr)),
                            null, 0, 10);
            assertEquals(parentNumber, searchResponse.getHits().getTotalHits());
        }

        Node node = runner.node();

        runner.ensureGreen();
        test_index_to_newIndex_pc(node, index, parentType, childType, ageStr);

        runner.ensureGreen();
        test_index_type_to_newIndex_pc(node, index, parentType, childType, ageStr);

        runner.ensureGreen();
        test_index_to_remote_newIndex_pc(node, index, parentType, childType, ageStr);

        runner.ensureGreen();
        test_index_type_to_remote_newIndex_pc(node, index, parentType,
                childType, ageStr);
    }

    private void test_index_to_newIndex_pc(Node node, String index, String parentType, String childType, String age) throws IOException {
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
            assertTrue(map.containsKey("acknowledged"));
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search parent documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newParentType, null, null, 0, 10);
            assertEquals(parentNumber, searchResponse.getHits().getTotalHits());
        }
        // search child documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newChildType, null, null, 0, 10);
            assertEquals(parentNumber * childNumber, searchResponse.getHits().getTotalHits());
        }
        // search a certain parent documents
        {
            final SearchResponse searchResponse = runner
                    .search(newIndex, newParentType, QueryBuilders
                                    .hasChildQuery(newChildType,
                                            QueryBuilders.matchQuery("age", age)),
                            null, 0, 10);
            assertEquals(parentNumber, searchResponse.getHits().getTotalHits());
        }
        runner.deleteIndex(newIndex);
    }

    private void test_index_type_to_newIndex_pc(Node node, String index, String parentType, String childType, String age) throws IOException {
        String newIndex = "company2";
        String newParentType = parentType;
        String newChildType = childType;

        // create an index
        runner.createIndex(newIndex, (Settings) null);
        runner.createMapping(newIndex, newChildType,
                "{\"_parent\":{\"type\":\"" + parentType + "\"}}");

        // reindex
        try (CurlResponse curlResponse = Curl
                .post(node, "/" + index + "/" + parentType + "," + childType + "/_reindex/" + newIndex + "/")
                .param("wait_for_completion", "true").execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(map.containsKey("acknowledged"));
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search parent documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newParentType, null, null, 0, 10);
            assertEquals(parentNumber, searchResponse.getHits().getTotalHits());
        }
        // search 1000 child documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newChildType, null, null, 0, 10);
            assertEquals(parentNumber * childNumber, searchResponse.getHits().getTotalHits());
        }
        // search a certain parent documents
        {
            final SearchResponse searchResponse = runner
                    .search(newIndex, newParentType, QueryBuilders
                                    .hasChildQuery(newChildType,
                                            QueryBuilders.matchQuery("age", age)),
                            null, 0, 10);
            assertEquals(parentNumber, searchResponse.getHits().getTotalHits());
        }
        runner.deleteIndex(newIndex);
    }

    private void test_index_to_remote_newIndex_pc(Node node, String index, String parentType, String childType, String age) throws IOException {
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
                .param("url", "http://localhost:" + node.settings().get("http.port"))
                .execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(map.containsKey("acknowledged"));
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search parent documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newParentType, null, null, 0, 10);
            assertEquals(parentNumber, searchResponse.getHits().getTotalHits());
        }
        // search child documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newChildType, null, null, 0, 10);
            assertEquals(parentNumber * childNumber, searchResponse.getHits().getTotalHits());
        }
        // search a certain parent documents
        {
            final SearchResponse searchResponse = runner
                    .search(newIndex, newParentType,
                            QueryBuilders.hasChildQuery(newChildType, QueryBuilders.matchQuery("age", age)),
                            null, 0, 10);
            assertEquals(parentNumber, searchResponse.getHits().getTotalHits());
        }
        runner.deleteIndex(newIndex);
    }

    private void test_index_type_to_remote_newIndex_pc(Node node, String index, String parentType, String childType, String age) throws IOException {
        String newIndex = "company2";
        String newParentType = parentType;
        String newChildType = childType;

        // create an index
        runner.createIndex(newIndex, (Settings) null);
        runner.createMapping(newIndex, newChildType,
                "{\"_parent\":{\"type\":\"" + parentType + "\"}}");

        // reindex
        try (CurlResponse curlResponse = Curl
                .post(node, "/" + index + "/" + parentType + "," + childType + "/_reindex/" + newIndex + "/")
                .param("wait_for_completion", "true")
                .param("url", "http://localhost:" + node.settings().get("http.port"))
                .execute()) {
            Map<String, Object> map = curlResponse.getContentAsMap();
            assertTrue(map.containsKey("acknowledged"));
            assertNull(map.get("name"));
        }

        runner.flush();

        assertTrue(runner.indexExists(index));
        assertTrue(runner.indexExists(newIndex));

        // search parent documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newParentType, null, null, 0, 10);
            assertEquals(parentNumber, searchResponse.getHits().getTotalHits());
        }
        // search child documents
        {
            final SearchResponse searchResponse = runner.search(newIndex,
                    newChildType, null, null, 0, 10);
            assertEquals(parentNumber * childNumber, searchResponse.getHits().getTotalHits());
        }
        // search a certain parent documents
        {
            final SearchResponse searchResponse = runner
                    .search(newIndex, newParentType,
                            QueryBuilders.hasChildQuery(newChildType, QueryBuilders.matchQuery("age", age)),
                            null, 0, 10);
            assertEquals(parentNumber, searchResponse.getHits().getTotalHits());
        }
        runner.deleteIndex(newIndex);
    }
}
