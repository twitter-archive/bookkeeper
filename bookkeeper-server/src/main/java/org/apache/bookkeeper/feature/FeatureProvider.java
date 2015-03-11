package org.apache.bookkeeper.feature;

/**
 * Provider to provide features.
 */
public interface FeatureProvider {
    /**
     * Return the feature with given name.
     *
     * @param name feature name
     * @return feature instance
     */
    Feature getFeature(String name);

    /**
     * Provide the feature provider under scope <i>name</i>.
     *
     * @param name
     *          scope name.
     * @return feature provider under scope <i>name</i>
     */
    FeatureProvider scope(String name);
}
