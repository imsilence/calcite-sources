package com.silence.calcite.sources.v3;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.CsvEnumerator;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Source;

import java.lang.reflect.Type;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class CSVTranslatableTable extends AbstractCSVTable implements QueryableTable, TranslatableTable {
    public CSVTranslatableTable(Source source, RelProtoDataType protoDataType) {
        super(source, protoDataType);
        log.info("init translatable table: {}", source.path());
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getElementType() {
        return Object[].class;
    }

    @Override
    public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
        return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        int fieldCount = relOptTable.getRowType().getFieldCount();
        int[] fields = CsvEnumerator.identityList(fieldCount);
        return new CSVTableScan(context.getCluster(), relOptTable, this, fields);
    }

    public Enumerable<Object> project(final DataContext context, int[] fields) {
        AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(context);
        return new AbstractEnumerable<Object>() {
            @Override
            public Enumerator<Object> enumerator() {
                return new CsvEnumerator(source, cancelFlag, getFieldTypes(context.getTypeFactory()), ImmutableIntList.of(fields));
            }
        };
    }
}
