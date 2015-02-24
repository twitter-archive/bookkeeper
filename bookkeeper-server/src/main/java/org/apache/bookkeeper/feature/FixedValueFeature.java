package org.apache.bookkeeper.feature;

public class FixedValueFeature implements Feature {
    protected int availability;

    public FixedValueFeature(int availability) {
        this.availability = availability;
    }

    public FixedValueFeature(boolean isAvailabile) {
        this.availability = isAvailabile ? FEATURE_AVAILABILITY_MAX_VALUE : 0;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public int availability() {
        return availability;
    }

    @Override
    public boolean isAvailable() {
        return availability() > 0;
    }
}
