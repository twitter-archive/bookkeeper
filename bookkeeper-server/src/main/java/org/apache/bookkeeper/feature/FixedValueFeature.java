package org.apache.bookkeeper.feature;

public class FixedValueFeature implements Feature {
    protected final String name;
    protected int availability;

    public FixedValueFeature(String name, int availability) {
        this.name = name;
        this.availability = availability;
    }

    public FixedValueFeature(String name, boolean available) {
        this.name = name;
        this.availability = available ? FEATURE_AVAILABILITY_MAX_VALUE : 0;
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
