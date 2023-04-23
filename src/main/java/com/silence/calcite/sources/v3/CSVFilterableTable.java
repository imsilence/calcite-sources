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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Source;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class CSVFilterableTable extends AbstractCSVTable implements FilterableTable {
    public CSVFilterableTable(Source source, RelProtoDataType protoDataType) {
        super(source, protoDataType);
        log.info("init filterable table: {}", source.path());
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        log.info("scan table: {}", source.path());
        JavaTypeFactory typeFactory = root.getTypeFactory();
        List<RelDataType> fieldTypes = getFieldTypes(typeFactory);
        List<Integer> fields = ImmutableIntList.identity(fieldTypes.size());
        String[] filterValues = new String[fieldTypes.size()];
        filters.removeIf(filter -> addFilter(filter, filterValues));
        AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new CsvEnumerator<>(source, cancelFlag, false, filterValues, CsvEnumerator.arrayConverter(fieldTypes, fields, false));
            }
        };
    }

    private boolean addFilter(RexNode filter, String[] filterValues) {
        if (filter.isA(SqlKind.AND)) {
            ((RexCall) filter).getOperands().forEach(subFilter -> addFilter(subFilter, filterValues));
        } else if (filter.isA(SqlKind.EQUALS)) {
            RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = ((RexCall) left).getOperands().get(0);
            }
            RexNode right = call.getOperands().get(1);
            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                int index = ((RexInputRef) left).getIndex();
                if (Objects.isNull(filterValues[index])) {
                    filterValues[index] = ((RexLiteral) right).getValue2().toString();
                    return true;
                }
            }
        }
        return false;
    }
}
