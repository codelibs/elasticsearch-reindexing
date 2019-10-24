package org.codelibs.elasticsearch.reindex.service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.codelibs.elasticsearch.reindex.exception.ReindexingException;
import org.codelibs.elasticsearch.runner.net.Curl;
import org.codelibs.elasticsearch.runner.net.CurlException;
import org.codelibs.elasticsearch.runner.net.CurlRequest;
import org.codelibs.elasticsearch.runner.net.CurlRequest.ConnectionBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * A LifecycleComponent realising all reindexing works
 */
public class ReindexingService extends AbstractLifecycleComponent<ReindexingService> {

    private Client client;

    private Map<String, ReindexingListener> reindexingListenerMap = new ConcurrentHashMap<String, ReindexingService.ReindexingListener>();

    private ThreadPool threadPool;

    @Inject
    public ReindexingService(final Settings settings, final Client client,
                             final ThreadPool threadPool) {
        super(settings);
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        if (logger.isDebugEnabled()) {
            logger.debug("Starting ReindexingService");
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("Stopping ReindexingService...");
        for (ReindexingListener listener : reindexingListenerMap.values()) {
            listener.interrupt();
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        // nothing
    }

    public boolean exists(final String name) {
        return reindexingListenerMap.containsKey(name);
    }

    public String[] getNames() {
        return reindexingListenerMap.keySet().toArray(
                new String[reindexingListenerMap.size()]);
    }

    public void delete(final String name) {
        final ReindexingListener reindexingListener = reindexingListenerMap
                .remove(name);
        if (reindexingListener != null) {
            reindexingListener.interrupt();
        } else {
            throw new ReindexingException("Reindexing process " + name
                    + " is not found.");
        }
    }

    /**
     * Execute the reindexing
     *
     * @param params   Rest request
     * @param content  Content of rest request in {}
     * @param listener is to receive the response back
     * @return
     */
    public String execute(final Params params, final BytesReference content, final ActionListener<Void> listener) {

        final String url = params.param("url");
        // set scroll to 1m if there is no
        final String scroll = params.param("scroll", "1m");
        final String fromIndex = params.param("index");
        final String fromType = params.param("type");
        final String toIndex = params.param("toindex");
        final String toType = params.param("totype");
        final String[] fields = params.paramAsBoolean("parent", true) ? new String[]{"_source", "_parent"} : new String[]{"_source"};
        final boolean deletion = params.paramAsBoolean("deletion", false);

        final ReindexingListener reindexingListener = new ReindexingListener(url, fromIndex, fromType, toIndex, toType, scroll, deletion, listener);

        // Create search request builder
        final SearchRequestBuilder builder = client.prepareSearch(fromIndex)
                .setScroll(scroll).addFields(fields);
        if (fromType != null && fromType.trim().length() > 0) {
            builder.setTypes(fromType.split(","));
        }
        if (content == null || content.length() == 0) {
            builder.setQuery(QueryBuilders.matchAllQuery()).setSize(
                    Integer.parseInt(params.param("size", "1000")));
        } else {
            builder.setExtraSource(content);
        }
        builder.execute(reindexingListener);  // async
        reindexingListenerMap.put(reindexingListener.getName(), reindexingListener);
        return reindexingListener.getName();
    }

    /**
     * An implementation of ActionListener to action for reindexing
     */
    private class ReindexingListener implements ActionListener<SearchResponse> {

        private AtomicBoolean interrupted = new AtomicBoolean(false);

        private String url;

        private String fromIndex;

        private String fromType;

        private String toIndex;

        private String toType;

        private String scroll;

        private String name;

        private ActionListener<Void> listener;

        private volatile String scrollId;

        private boolean deletion;

        ReindexingListener(final String url, final String fromIndex, final String fromType, final String toIndex, final String toType, final String scroll, final boolean deletion, final ActionListener<Void> listener) {
            if (toIndex == null) {
                throw new ReindexingException("toindex is blank.");
            }
            this.url = url != null && !url.endsWith("/") ? url + "/" : url;
            this.fromIndex = fromIndex;
            this.fromType = fromType;
            this.toIndex = toIndex;
            this.toType = toType;
            this.scroll = scroll;
            this.deletion = deletion;
            this.listener = listener;
            this.name = UUID.randomUUID().toString();
        }

        /**
         * Action on the response
         *
         * @param response
         */
        @Override
        public void onResponse(final SearchResponse response) {
            if (interrupted.get()) {
                listener.onFailure(new ReindexingException("Interrupted."));
                return;
            }

            // Get 10 hit results
            final SearchHits searchHits = response.getHits();
            final SearchHit[] hits = searchHits.getHits();
            if (hits.length == 0) { // finished
                scrollId = null;
                reindexingListenerMap.remove(name);
                if (deletion) {
                    if (fromType == null)
                        deleteIndex(fromIndex);
                    else
                        deleteIndexType(fromIndex, fromType);
                }
                listener.onResponse(null);
            } else {
                scrollId = response.getScrollId();
                if (url != null) {
                    threadPool.generic().execute(new Runnable() {
                        @Override
                        public void run() {
                            sendToRemoteCluster(scrollId, hits);
                        }
                    });
                } else {
                    sendToLocalCluster(scrollId, hits);
                }
            }
        }

        private void sendToLocalCluster(final String scrollId, final SearchHit[] hits) {

            // prepare bulk request
            final BulkRequestBuilder bulkRequest = client.prepareBulk();
            for (final SearchHit hit : hits) {
                IndexRequestBuilder builder = client.prepareIndex(toIndex,
                        toType != null ? toType : hit.getType(), hit.getId())
                        .setSource(hit.getSource());
                Map<String, SearchHitField> fields = hit.getFields();
                if (fields != null && fields.containsKey("_parent")) {
                    SearchHitField parentField = fields.get("_parent");
                    if (parentField != null) {
                        String parentId = parentField.getValue();
                        builder.setParent(parentId);
                    }
                }
                bulkRequest.add(builder);
            }

            // send bulk request, if success response got, searching the next 10 results using scroll_id
            // using this listener (inner class) to listen to results
            bulkRequest.execute(new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(final BulkResponse bulkResponse) {
                    if (bulkResponse.hasFailures()) {
                        throw new ReindexingException(bulkResponse
                                .buildFailureMessage());
                    }
                    client.prepareSearchScroll(scrollId).setScroll(scroll)
                            .execute(ReindexingListener.this);
                }

                @Override
                public void onFailure(final Throwable e) {
                    ReindexingListener.this.onFailure(e);
                }
            });
        }

        private void sendToRemoteCluster(final String scrollId, final SearchHit[] hits) {
            try {
                Curl.post(url + "_bulk").onConnect(new ConnectionBuilder() {
                    @Override
                    public void onConnect(CurlRequest curlRequest,
                                          HttpURLConnection connection) {
                        connection.setDoOutput(true);
                        try (BufferedWriter writer = new BufferedWriter(
                                new OutputStreamWriter(connection
                                        .getOutputStream(), curlRequest
                                        .encoding()))) {
                            StringBuilder buf = new StringBuilder(200);
                            for (final SearchHit hit : hits) {
                                String source = hit.getSourceAsString();
                                if (source != null) {
                                    buf.setLength(0);
                                    buf.append("{\"index\":{\"_index\":\"");
                                    buf.append(toIndex);
                                    buf.append("\",\"_type\":\"");
                                    if (toType == null) {
                                        buf.append(hit.getType());
                                    } else {
                                        buf.append(toType);
                                    }
                                    buf.append("\",\"_id\":\"");
                                    buf.append(hit.getId());
                                    buf.append("\"");
                                    Map<String, SearchHitField> fields = hit
                                            .getFields();
                                    if (fields != null
                                            && fields.containsKey("_parent")) {
                                        SearchHitField parentField = fields
                                                .get("_parent");
                                        if (parentField != null) {
                                            String parentId = parentField
                                                    .getValue();
                                            buf.append(",\"_parent\":\"");
                                            buf.append(parentId);
                                            buf.append("\"");
                                        }
                                    }
                                    buf.append("}}");
                                    writer.write(buf.toString());
                                    writer.write("\n");
                                    writer.write(source);
                                    writer.write("\n");
                                }
                            }
                            writer.flush();
                        } catch (IOException e) {
                            ReindexingListener.this.onFailure(e);
                        }
                    }
                }).execute(new Curl.ResponseListener() {
                    @Override
                    public void onResponse(HttpURLConnection con) {
                        try {
                            int responseCode = con.getResponseCode();
                            if (responseCode == 200) {
                                client.prepareSearchScroll(scrollId)
                                        .setScroll(scroll)
                                        .execute(ReindexingListener.this);
                            } else {
                                throw new ReindexingException(
                                        "The response code from " + url + " is");
                            }
                        } catch (Exception e) {
                            ReindexingListener.this.onFailure(e);
                        }
                    }
                });
            } catch (CurlException e) {
                onFailure(e);
            }
        }

        private void deleteIndex(final String fromIndex) {
            try {
                // sync
                client.admin().indices().delete(new DeleteIndexRequest(fromIndex)).get();
            } catch (InterruptedException e) {
                System.err.println("interrupted while deleting index " + fromIndex);
                e.printStackTrace();
            } catch (ExecutionException e) {
                System.err.println("failed to deleting index " + fromIndex);
                e.printStackTrace();
            }
        }

        private void deleteIndexType(final String fromIndex, final String fromType) {
            final SearchRequestBuilder builder = client.prepareSearch(fromIndex).setTypes(fromType).setScroll("1m");
            SearchResponse searchResponse = builder.get();
            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit hit : hits)
                client.prepareDelete(hit.index(), hit.type(), hit.id()).get();
            while (hits.length != 0) {
                searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()).setScroll("1m").get();
                hits = searchResponse.getHits().getHits();
                for (SearchHit hit : hits)
                    client.prepareDelete(hit.index(), hit.type(), hit.id()).get();
            }
        }

        @Override
        public void onFailure(final Throwable e) {
            logger.error("Failed to reindex {}.", toIndex, e);
            delete(name);
            listener.onFailure(e);
        }

        public void interrupt() {
            interrupted.set(true);
            if (scrollId != null) {
                client.prepareClearScroll().addScrollId(scrollId)
                        .execute(new ActionListener<ClearScrollResponse>() {

                            @Override
                            public void onResponse(ClearScrollResponse response) {
                                // nothing
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug(
                                            "Failed to stop reindexing for "
                                                    + toIndex + ".", e);
                                }
                            }
                        });
            }
        }

        public String getName() {
            return name;
        }
    }
}
