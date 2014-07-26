package org.codelibs.elasticsearch.reindex.rest;

import static org.elasticsearch.rest.RestStatus.OK;

import java.util.LinkedHashMap;
import java.util.Map;

import org.codelibs.elasticsearch.reindex.exception.ReindexingException;
import org.codelibs.elasticsearch.reindex.service.ReindexingService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

public class ReindexRestAction extends BaseRestHandler {

    private ReindexingService reindexingService;

    @Inject
    public ReindexRestAction(final Settings settings, final Client client,
            final RestController restController,
            final ReindexingService reindexingService) {
        super(settings, client);
        this.reindexingService = reindexingService;

        restController.registerHandler(RestRequest.Method.GET,
                "/_reindex", this);
        restController.registerHandler(RestRequest.Method.GET,
                "/_reindex/{name}", this);

        restController.registerHandler(RestRequest.Method.POST,
                "/{index}/{type}/_reindex/{toindex}/{totype}", this);
        restController.registerHandler(RestRequest.Method.POST,
                "/{index}/{type}/_reindex/{toindex}", this);
        restController.registerHandler(RestRequest.Method.POST,
                "/{index}/_reindex/{toindex}/{totype}", this);
        restController.registerHandler(RestRequest.Method.POST,
                "/{index}/_reindex/{toindex}", this);

        restController.registerHandler(RestRequest.Method.DELETE,
                "/_reindex/{name}", this);
    }

    @Override
    protected void handleRequest(final RestRequest request,
            final RestChannel channel, final Client client) throws Exception {
        String name;
        Map<String, Object> params;
        try {
            switch (request.method()) {
            case GET:
                name = request.param("name");
                params = new LinkedHashMap<String, Object>();
                if (name == null) {
                    params.put("names", reindexingService.getNames());
                } else {
                    params.put("name", name);
                    params.put("found", reindexingService.exists(name));
                }
                sendResponse(channel, params);
                break;
            case POST:
                final boolean waitForCompletion = request.paramAsBoolean(
                        "wait_for_completion", false);
                name = reindexingService.execute(request,
                        request.hasContent() ? request.content() : null,
                        new ActionListener<Void>() {
                            @Override
                            public void onResponse(final Void response) {
                                if (waitForCompletion) {
                                    sendResponse(channel, null);
                                }
                            }

                            @Override
                            public void onFailure(final Throwable e) {
                                if (waitForCompletion) {
                                    sendErrorResponse(channel, e);
                                }
                            }
                        });
                if (!waitForCompletion) {
                    params = new LinkedHashMap<String, Object>();
                    params.put("name", name);
                    sendResponse(channel, params);
                }
                break;
            case DELETE:
                name = request.param("name");
                params = new LinkedHashMap<String, Object>();
                params.put("name", name);
                reindexingService.delete(name);
                sendResponse(channel, params);
                break;
            default:
                sendErrorResponse(channel, new ReindexingException(
                        "Invalid request: " + request));
                break;
            }
        } catch (final Exception e) {
            sendErrorResponse(channel, e);
        }
    }

    private void sendResponse(final RestChannel channel,
            final Map<String, Object> params) {
        try {
            final XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            builder.field("acknowledged", true);
            if (params != null) {
                for (final Map.Entry<String, Object> entry : params.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(OK, builder));
        } catch (final Exception e) {
            sendErrorResponse(channel, e);
        }
    }

    private void sendErrorResponse(final RestChannel channel, final Throwable t) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, t));
        } catch (final Exception e) {
            logger.error(
                    "Failed to send a failure response: " + t.getMessage(), e);
        }
    }
}
