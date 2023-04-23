package com.silence.calcite.sources.v4;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

public class RelAlgebra {

    public static Schema getSchema(SchemaPlus parentSchema) {
        String name = "iam";
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("jdbcUrl", "jdbc:mariadb://localhost:3306/iam");
        builder.put("jdbcDriver", "org.mariadb.jdbc.Driver");
        builder.put("jdbcUser", "root");
        builder.put("jdbcPassword", "881019");
        builder.put("jdbcSchema", name);
        return JdbcSchema.create(parentSchema, name, builder.build());
    }

    public static FrameworkConfig config() {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        SchemaPlus schema = rootSchema.add("iam", getSchema(rootSchema));

        return Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(schema)
                .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2)).build();
    }

    public static void print(RelNode node) {
        System.out.println(RelOptUtil.toString(node));
    }

    public static void main(String[] args) {
        RelBuilder builder = RelBuilder.create(config());
        print(builder.scan("users").build());
        print(builder.scan("users").project(builder.field("id")).build());
        print(builder.scan("users").filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field("group"), builder.literal("g1"))).project(builder.field("id")).build());
        print(builder.scan("users").filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field("group"), builder.literal("g1"))).aggregate(builder.groupKey("role"), builder.count(false, "cnt")).build());
        print(builder.scan("users").scan("roles").join(JoinRelType.LEFT, builder.equals(builder.field(2, 0, "group"), builder.field(2, 1, "id"))).build());

    }
}
