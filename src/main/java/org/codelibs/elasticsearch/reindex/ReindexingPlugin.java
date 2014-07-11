package org.codelibs.elasticsearch.reindex;

import java.util.Collection;

import org.codelibs.elasticsearch.reindex.module.ReindexingModule;
import org.codelibs.elasticsearch.reindex.rest.ReindexRestAction;
import org.codelibs.elasticsearch.reindex.service.ReindexingService;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

public class ReindexingPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "ReindexingPlugin";
    }

    @Override
    public String description() {
        return "This is a elasticsearch-reindexing plugin.";
    }

    // for Rest API
    public void onModule(final RestModule module) {
        module.addRestAction(ReindexRestAction.class);
    }

    // for Service
    @Override
    public Collection<Class<? extends Module>> modules() {
        final Collection<Class<? extends Module>> modules = Lists
                .newArrayList();
        modules.add(ReindexingModule.class);
        return modules;
    }

    // for Service
    @SuppressWarnings("rawtypes")
    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        final Collection<Class<? extends LifecycleComponent>> services = Lists
                .newArrayList();
        services.add(ReindexingService.class);
        return services;
    }
}
