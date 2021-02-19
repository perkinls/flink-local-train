package com.lp.java.demo.commons;

/**
 * @author li.pan
 * @version 1.0.0
 * @Description base抽象类
 * @createTime 2021年02月19日 17:32:00
 */
public interface IBaseRunApp {
    /**
     * 所有任务的业务逻辑处理地方
     *
     * @throws Exception
     */
    void doMain() throws Exception;
}
