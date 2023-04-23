package com.silence.calcite.sources.v3;

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.util.Sources;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.util.Map;
import java.util.Objects;

public class CSVTableFactory implements TableFactory {
    @Override
    public Table create(SchemaPlus schema, String name, Map operand, @Nullable RelDataType rowType) {
        File base = (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
        String path = (String) operand.get("path");
        String flavorName = (String) operand.getOrDefault("flavor", "");
        File file = new File(path);
        if (Objects.nonNull(base) && !file.isAbsolute()) {
            file = new File(base, path);
        }
        RelProtoDataType relProtoDataType = Objects.nonNull(rowType) ? RelDataTypeImpl.proto(rowType) : null;
        return Flavor.get(flavorName).getTable(Sources.of(file), relProtoDataType);
    }
}
