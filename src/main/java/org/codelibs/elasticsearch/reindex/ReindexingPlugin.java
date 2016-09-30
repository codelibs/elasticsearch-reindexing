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
 * 开发插件方法:继承并实现{@link Plugin}类
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
     * ES集群启动时,会使用Guice的Injector来创建各个模块(module)
     * 模块中绑定了许多服务
     *
     * @param module Elasticsearch中的module实际上就是Guice中的module,
     *               即用来定义绑定规则(接口到实现),
     */
    public void onModule(final RestModule module) {
        module.addRestAction(ReindexRestAction.class);
    }

    // for Service
    @Override
    public Collection<Module> nodeModules() {
        final Collection<Module> modules =new ArrayList<>();
        modules.add(new ReindexingModule());
        return modules;
    }

    // for Service
    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        final Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        services.add(ReindexingService.class);
        return services;
    }
}
