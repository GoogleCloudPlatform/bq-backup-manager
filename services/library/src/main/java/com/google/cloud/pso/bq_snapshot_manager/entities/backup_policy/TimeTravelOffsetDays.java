package com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy;

import java.util.Arrays;

public enum TimeTravelOffsetDays {
    DAYS_0("0"),
    DAYS_1("1"),
    DAYS_2("2"),
    DAYS_3("3"),
    DAYS_4("4"),
    DAYS_5("5"),
    DAYS_6("6"),
    DAYS_7("7");

    private String text;

    TimeTravelOffsetDays(String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }

    public static TimeTravelOffsetDays fromString(String text) throws IllegalArgumentException {
        for (TimeTravelOffsetDays b : TimeTravelOffsetDays.values()) {
            if (b.text.equalsIgnoreCase(text)) {
                return b;
            }
        }
        throw new IllegalArgumentException(
                String.format("Invalid enum text '%s'. Available values are '%s'",
                        text,
                        Arrays.asList(TimeTravelOffsetDays.values())
                )
        );
    }
}
