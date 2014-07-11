package org.codelibs.elasticsearch.reindex.exception;

public class ReindexingException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ReindexingException(final String message) {
        super(message);
    }

    public ReindexingException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
