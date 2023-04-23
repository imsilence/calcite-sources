package com.silence.calcite.sources.v6;

import org.apache.calcite.linq4j.function.Parameter;

import java.util.Locale;

public class MaskingFunction {
    public String eval(@Parameter(name = "x") String x) {
        return String.format(Locale.ENGLISH, "****%s****", x);
    }
}
