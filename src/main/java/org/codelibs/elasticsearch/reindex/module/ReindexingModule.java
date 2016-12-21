package org.codelibs.elasticsearch.reindex.module;

import org.codelibs.elasticsearch.reindex.service.ReindexingService;
import org.elasticsearch.common.inject.AbstractModule;

/**
 * A Guice module which bind the interface with the implementation
 * {@link ReindexingModule} defines that {@link ReindexingService} is Singleton
 *
 */
public class ReindexingModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ReindexingService.class).asEagerSingleton();
    }
}