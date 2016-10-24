package org.apache.bookkeeper.feature;

/**
 * A provider will provide settable features.
 */
public class SettableFeatureProvider extends CacheableFeatureProvider<SettableFeature> {

    public final static FeatureProvider DISABLE_ALL = new SettableFeatureProvider("", 0);

    protected final int availability;

    public SettableFeatureProvider(String scope, int availability) {
        super(scope);
        this.availability = availability;
    }

    @Override
    protected SettableFeature makeFeature(String featureName) {
        return new SettableFeature(featureName, availability);
    }

    @Override
    protected FeatureProvider makeProvider(String fullScopeName) {
        return new SettableFeatureProvider(fullScopeName, availability);
    }

}
