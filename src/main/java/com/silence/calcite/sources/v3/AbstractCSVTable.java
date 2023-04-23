package com.silence.calcite.sources.v3;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.file.CsvEnumerator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Slf4j
public class AbstractCSVTable extends AbstractTable {
    protected final Source source;
    private List<RelDataType> fieldTypes;
    private RelDataType rowDataType;
    private RelProtoDataType protoDataType;

    public AbstractCSVTable(Source source, RelProtoDataType protoDataType) {
        this.source = source;
        this.protoDataType = protoDataType;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (Objects.isNull(rowDataType)) {
            if (Objects.nonNull(protoDataType)) {
                rowDataType = protoDataType.apply(typeFactory);
            } else {
                rowDataType = CsvEnumerator.deduceRowType((JavaTypeFactory) typeFactory, source, null, false);
            }

        }
        log.info("get row type({}): {}", source.path(), rowDataType);
        return rowDataType;
    }

    protected List<RelDataType> getFieldTypes(JavaTypeFactory typeFactory) {
        if (Objects.isNull(fieldTypes)) {
            fieldTypes = new ArrayList<>();
            CsvEnumerator.deduceRowType(typeFactory, source, fieldTypes, false);
        }
        log.info("get field types({}): {}", source.path(), fieldTypes);
        return fieldTypes;
    }
}
