package com.silence.calcite.sources.entities;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@AllArgsConstructor
@ToString
@EqualsAndHashCode(of = {"id"})
public class Role {
    public final int id;
    public final String name;
}
