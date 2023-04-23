package com.silence.calcite.sources.entities;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.calcite.adapter.java.Array;

import java.util.List;

@AllArgsConstructor
@ToString
@EqualsAndHashCode(of = {"id"})
public class Group {
    public final int id;
    public final String name;
    @Array(component = User.class)
    public final List<User> users;
}
