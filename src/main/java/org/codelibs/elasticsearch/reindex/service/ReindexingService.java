package org.codelibs.elasticsearch.reindex.service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.codelibs.elasticsearch.reindex.exception.ReindexingException;
import org.codelibs.elasticsearch.util.lang.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class ReindexingService extends
        AbstractLifecycleComponent<ReindexingService> {

    private Client client;

    private Map<String, ReindexingListener> reindexingListenerMap = new ConcurrentHashMap<String, ReindexingService.ReindexingListener>();;

    @Inject
    public ReindexingService(final Settings settings, final Client client) {
        super(settings);
        this.client = client;
        logger.info("CREATE ReindexingService");
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.info("START ReindexingService");
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("STOP ReindexingService");
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.info("CLOSE ReindexingService");
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

    public String execute(final Params params, final BytesReference content,
            final ActionListener<Void> listener) {
        final String scroll = params.param("scroll");
        final String fromIndex = params.param("index");
        final String fromType = params.param("type");
        final String toIndex = params.param("toindex");
        final String toType = params.param("totype");
        final ReindexingListener reindexingListener = new ReindexingListener(
                toIndex, toType, scroll, listener);
        final SearchRequestBuilder builder = client.prepareSearch(fromIndex)
                .setSearchType(SearchType.SCAN).setScroll(scroll)
                .setListenerThreaded(true);
        if (StringUtils.isNotBlank(fromType)) {
            builder.setTypes(fromType.split(","));
        }
        if (content == null) {
            builder.setQuery(QueryBuilders.matchAllQuery()).setSize(
                    Integer.parseInt(params.param("size", "1000")));
        } else {
            builder.setSource(content);
        }
        builder.execute(reindexingListener);
        reindexingListenerMap.put(reindexingListener.getName(),
                reindexingListener);
        return reindexingListener.getName();
    }

    private class ReindexingListener implements ActionListener<SearchResponse> {
        private AtomicBoolean initialized = new AtomicBoolean(false);

        private AtomicBoolean interrupted = new AtomicBoolean(false);

        private String toIndex;

        private String toType;

        private String scroll;

        private String name;

        private ActionListener<Void> listener;

        ReindexingListener(final String toIndex, final String toType,
                final String scroll, final ActionListener<Void> listener) {
            this.toIndex = toIndex;
            this.toType = toType;
            this.scroll = scroll;
            this.listener = listener;
            if (toIndex == null) {
                throw new ReindexingException("toindex is blank.");
            }
            name = UUID.randomUUID().toString();
        }

        @Override
        public void onResponse(final SearchResponse response) {
            if (interrupted.get()) {
                listener.onFailure(new ReindexingException("Interrupted."));
                return;
            }

            if (initialized.compareAndSet(false, true)) {
                client.prepareSearchScroll(response.getScrollId())
                        .setScroll(scroll).setListenerThreaded(true)
                        .execute(this);
                return;
            }

            final SearchHits searchHits = response.getHits();
            final SearchHit[] hits = searchHits.getHits();
            if (hits.length == 0) {
                delete(name);
                listener.onResponse(null);
            } else {
                final BulkRequestBuilder bulkRequest = client.prepareBulk();
                for (final SearchHit hit : hits) {
                    bulkRequest.add(client.prepareIndex(toIndex,
                            toType != null ? toType : hit.getType(),
                            hit.getId()).setSource(hit.getSource()));
                }

                bulkRequest.execute(new ActionListener<BulkResponse>() {

                    @Override
                    public void onResponse(final BulkResponse bulkResponse) {
                        if (bulkResponse.hasFailures()) {
                            throw new ReindexingException(bulkResponse
                                    .buildFailureMessage());
                        }
                        client.prepareSearchScroll(response.getScrollId())
                                .setScroll(scroll).setListenerThreaded(true)
                                .execute(ReindexingListener.this);
                    }

                    @Override
                    public void onFailure(final Throwable e) {
                        ReindexingListener.this.onFailure(e);
                    }
                });
            }
        }

        @Override
        public void onFailure(final Throwable e) {
            logger.error("Failed to reindex " + toIndex + ".", e);
            delete(name);
            listener.onFailure(e);
        }

        public void interrupt() {
            interrupted.set(true);
        }

        public String getName() {
            return name;
        }
    }
}
