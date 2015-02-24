package org.apache.bookkeeper.feature;

public class SettableFeature extends FixedValueFeature {
    public SettableFeature(int initialAvailability) {
        super(initialAvailability);
    }

    public SettableFeature(boolean isAvailabile) {
        super(isAvailabile);
    }

    public void set(int availability) {
        this.availability = availability;
    }

    public void set(boolean isAvailabile) {
        this.availability = isAvailabile ? FEATURE_AVAILABILITY_MAX_VALUE : 0;
    }

}
