package com.silence.calcite.sources.v3;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

import java.util.List;
import java.util.Objects;

@Value.Enclosing
public class CSVProjectTableScanRule extends RelRule<CSVProjectTableScanRule.Config> {
    protected CSVProjectTableScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        CSVTableScan scan = call.rel(1);
        int[] fields = getProjectFields(project.getProjects());
        if (Objects.nonNull(fields)) {
            call.transformTo(new CSVTableScan(scan.getCluster(), scan.getTable(), scan.getCsvTable(), fields));
        }
    }

    private int[] getProjectFields(List<RexNode> projects) {
        int[] fields = new int[projects.size()];
        for (int i = 0; i < projects.size(); i++) {
            RexNode node = projects.get(i);
            if (node instanceof RexInputRef) {
                fields[i] = ((RexInputRef) node).getIndex();
            } else {
                return null;
            }
        }
        return fields;
    }


    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableCSVProjectTableScanRule.Config.builder().build().withOperandSupplier(b0 -> b0.operand(LogicalProject.class).oneInput(b1 -> b1.operand(CSVTableScan.class).noInputs()));

        @Override
        default CSVProjectTableScanRule toRule() {
            return new CSVProjectTableScanRule(this);
        }
    }
}
