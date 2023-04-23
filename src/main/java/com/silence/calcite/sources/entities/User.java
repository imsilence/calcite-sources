package com.silence.calcite.sources.entities;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@AllArgsConstructor
@ToString
@EqualsAndHashCode(of = {"id"})
public class User {
    public final int id;
    public final String name;
    public final Role role;
}
