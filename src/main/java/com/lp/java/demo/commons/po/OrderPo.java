package com.lp.java.demo.commons.po;

/**
 * @author li.pan
 * @title
 * @Date 2021/12/2
 */
public class OrderPo {
    public Long user;
    public String product;
    public int amount;

    public OrderPo() {
    }

    public OrderPo(Long user, String product, int amount) {
        this.user = user;
        this.product = product;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Order{" +
                "user=" + user +
                ", product='" + product + '\'' +
                ", amount=" + amount +
                '}';
    }
}
