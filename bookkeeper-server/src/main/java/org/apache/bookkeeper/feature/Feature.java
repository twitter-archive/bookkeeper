package org.apache.bookkeeper.feature;

/**
 * This interface represents a feature.
 */
public interface Feature {
    public static int FEATURE_AVAILABILITY_MAX_VALUE = 100;

    /**
     * Returns a textual representation of the feature.
     *
     * @return name of the feature.
     */
    String name();

    /**
     * Returns the availability of this feature, an integer between 0 and 100.
     *
     * @return the availability of this feature.
     */
    int availability();

    /**
     * Whether this feature is available or not.
     *
     * @return true if this feature is available, otherwise false.
     */
    boolean isAvailable();
}

