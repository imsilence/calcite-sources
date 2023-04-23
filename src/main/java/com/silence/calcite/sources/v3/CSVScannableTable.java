package com.silence.calcite.sources.v3;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.CsvEnumerator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Source;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class CSVScannableTable extends AbstractCSVTable implements ScannableTable {
    public CSVScannableTable(Source source, RelProtoDataType protoDataType) {
        super(source, protoDataType);
        log.info("init scannable table: {}", source.path());
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        log.info("scan table: {}", source.path());
        JavaTypeFactory typeFactory = root.getTypeFactory();
        List<RelDataType> fieldTypes = getFieldTypes(typeFactory);
        List<Integer> fields = ImmutableIntList.identity(fieldTypes.size());
        AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new CsvEnumerator<>(source, cancelFlag, false, null, CsvEnumerator.arrayConverter(fieldTypes, fields, false));
            }
        };
    }
}
