package org.codelibs.elasticsearch.reindex.module;

import org.codelibs.elasticsearch.reindex.service.ReindexingService;
import org.elasticsearch.common.inject.AbstractModule;

public class ReindexingModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ReindexingService.class).asEagerSingleton();
    }
}