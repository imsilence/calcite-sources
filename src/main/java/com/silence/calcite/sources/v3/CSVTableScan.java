package com.silence.calcite.sources.v3;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class CSVTableScan extends TableScan implements EnumerableRel {
    @Getter
    private final CSVTranslatableTable csvTable;
    private final int[] fields;

    public CSVTableScan(RelOptCluster cluster, RelOptTable table, CSVTranslatableTable csvTable, int[] fields) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), ImmutableList.of(), table);
        this.csvTable = csvTable;
        this.fields = fields;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new CSVTableScan(getCluster(), table, csvTable, fields);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("fields", Primitive.asList(fields));
    }

    @Override
    public RelDataType deriveRowType() {
        List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
        RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        for (int field : fields) {
            builder.add(fieldList.get(field));
        }
        return builder.build();
    }

    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(CSVProjectTableScanRule.Config.DEFAULT.toRule());
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy((fields.length + 2.0) / (table.getRowType().getFieldCount() + 2.0));
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());
        return implementor.result(physType, Blocks.toBlock(Expressions.call(table.getExpression(CSVTranslatableTable.class), "project", implementor.getRootExpression(), Expressions.constant(fields))));
    }
}
