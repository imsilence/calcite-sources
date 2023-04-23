package com.silence.calcite.sources.v6;

public class SumFunction {
    public long init() {
        return 0L;
    }

    public long add(long accumulator, int v) {
        return accumulator + v;
    }

    public long merge(long accumulator0, long accumulator1) {
        return accumulator0 + accumulator1;
    }

    public long result(long accumulator) {
        return accumulator;
    }
}
