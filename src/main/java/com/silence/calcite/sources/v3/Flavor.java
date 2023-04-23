package com.silence.calcite.sources.v3;

import lombok.AllArgsConstructor;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Source;

import java.util.function.BiFunction;

@AllArgsConstructor
public enum Flavor {
    SCANNABLE("SCANNABLE", CSVScannableTable::new),
    FILTERABLE("FILTERABLE", CSVFilterableTable::new),
    TRANSLATABLE("TRANSLATABLE", CSVTranslatableTable::new);

    private final String name;
    private final BiFunction<Source, RelProtoDataType, AbstractCSVTable> function;

    public static Flavor get(String name) {
        for (Flavor flavor : Flavor.values()) {
            if (flavor.name.equals(name)) {
                return flavor;
            }
        }
        return SCANNABLE;
    }

    public Table getTable(Source source, RelProtoDataType relProtoDataType) {
        return function.apply(source, relProtoDataType);
    }

    public Table getTable(Source source) {
        return getTable(source, null);
    }
}
