package org.codelibs.elasticsearch.reindex;

import java.util.ArrayList;
import java.util.Collection;

import org.codelibs.elasticsearch.reindex.module.ReindexingModule;
import org.codelibs.elasticsearch.reindex.rest.ReindexRestAction;
import org.codelibs.elasticsearch.reindex.service.ReindexingService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;

/**
 * Plugin: An extension point allowing to plug in custom functionality.
 */
public class ReindexingPlugin extends Plugin {
    @Override
    public String name() {
        return "reindexing";
    }

    @Override
    public String description() {
        return "This plugin copies a new index by reindexing.";
    }

    /**
     * extend the given module
     * @param module
     */
    public void onModule(final RestModule module) {
        module.addRestAction(ReindexRestAction.class);
    }

    /**
     * extend node level modules
     * @return
     */
    @Override
    public Collection<Module> nodeModules() {
        final Collection<Module> modules =new ArrayList<>();
        modules.add(new ReindexingModule());
        return modules;
    }

    /**
     * extend node level services
     * @return
     */
    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        final Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        services.add(ReindexingService.class);
        return services;
    }
}
