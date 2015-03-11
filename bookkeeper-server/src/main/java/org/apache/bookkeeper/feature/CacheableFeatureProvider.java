package org.apache.bookkeeper.feature;

import org.apache.commons.lang.StringUtils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Cacheable Feature Provider
 */
public abstract class CacheableFeatureProvider<T extends Feature> implements FeatureProvider {

    protected final String scope;
    protected final ConcurrentMap<String, FeatureProvider> scopes =
            new ConcurrentHashMap<String, FeatureProvider>();
    protected final ConcurrentMap<String, T> features =
            new ConcurrentHashMap<String, T>();

    protected CacheableFeatureProvider(String scope) {
        this.scope = scope;
    }

    protected String makeName(String name) {
        if (StringUtils.isBlank(scope)) {
            return name;
        } else {
            return scope + "." + name;
        }
    }

    @Override
    public T getFeature(String name) {
        T feature = features.get(name);
        if (null == feature) {
            T newFeature = makeFeature(makeName(name));
            T oldFeature = features.putIfAbsent(name, newFeature);
            if (null == oldFeature) {
                feature = newFeature;
            } else {
                feature = oldFeature;
            }
        }
        return feature;
    }

    protected abstract T makeFeature(String featureName);

    @Override
    public FeatureProvider scope(String name) {
        FeatureProvider provider = scopes.get(name);
        if (null == provider) {
            FeatureProvider newProvider = makeProvider(makeName(name));
            FeatureProvider oldProvider = scopes.putIfAbsent(name, newProvider);
            if (null == oldProvider) {
                provider = newProvider;
            } else {
                provider = oldProvider;
            }
        }
        return provider;
    }

    protected abstract FeatureProvider makeProvider(String fullScopeName);
}
