package org.apache.bookkeeper.feature;

public class SettableFeature extends FixedValueFeature {
    public SettableFeature(String name, int initialAvailability) {
        super(name, initialAvailability);
    }

    public SettableFeature(String name, boolean isAvailabile) {
        super(name, isAvailabile);
    }

    public void set(int availability) {
        this.availability = availability;
    }

    public void set(boolean isAvailabile) {
        this.availability = isAvailabile ? FEATURE_AVAILABILITY_MAX_VALUE : 0;
    }

}
