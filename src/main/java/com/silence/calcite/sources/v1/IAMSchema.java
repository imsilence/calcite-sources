package com.silence.calcite.sources.v1;

import com.google.common.collect.ImmutableList;
import com.silence.calcite.sources.commons.AbstractBaseSchema;
import com.silence.calcite.sources.entities.Group;
import com.silence.calcite.sources.entities.Role;
import com.silence.calcite.sources.entities.User;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class IAMSchema extends AbstractBaseSchema {
    public final Role[] roles = {
            new Role(1, "root"),
            new Role(2, "admin"),
            new Role(3, "user"),
    };

    public final User[] users = {
            new User(1, "root", roles[0]),
            new User(2, "silence", roles[1]),
            new User(3, "kk", roles[2]),
            new User(4, "ada", roles[2]),
    };

    public final Group[] groups = {
            new Group(1, "g1", Arrays.asList(users[0], users[1])),
            new Group(2, "g2", Arrays.asList(users[2], users[3])),
    };


    @Override
    public Schema getSchema(SchemaPlus parentSchema) {
        return new ReflectiveSchema(new IAMSchema());
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