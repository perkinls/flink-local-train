package com.lp.java.demo.table.sql.udfs;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author li.pan
 * @title
 * @Date 2021/12/15
 */
public class ScalarFunctionExample extends ScalarFunction {

    private static final long serialVersionUID = 7975281977670535291L;

    public Integer eval(Integer a, Integer b) {
        return a + b;
    }

    public Integer eval(String a, String b) {
        return Integer.valueOf(a) + Integer.valueOf(b);
    }

    public Integer eval(Double... d) {
        double result = 0;
        for (double value : d)
            result += value;
        return (int) result;
    }
}
