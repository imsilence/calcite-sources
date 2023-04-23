package com.silence.calcite.sources.v2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.silence.calcite.sources.commons.AbstractBaseSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.SQLException;
import java.util.List;

public class IAMSchema extends AbstractBaseSchema {
    @Override
    public Schema getSchema(SchemaPlus parentSchema) {
        String name = getName();
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("jdbcUrl", "jdbc:mariadb://localhost:3306/iam");
        builder.put("jdbcDriver", "org.mariadb.jdbc.Driver");
        builder.put("jdbcUser", "root");
        builder.put("jdbcPassword", "881019");
//        builder.put("jdbcCatalog", null);
        builder.put("jdbcSchema", name);
        return JdbcSchema.create(parentSchema, name, builder.build());
    }

    @Override
    public String getName() {
        return "iam";
    }

    @Override
    public List<String> getQuerys() {
        ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
        builder.add("select * from `iam`.`roles`");
        builder.add("select * from `iam`.`users`");
        builder.add("select * from `iam`.`groups`");
        return builder.build();
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        new IAMSchema().exec();
    }
}
