package com.lp.test.asyncio;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

/**
 * <p/>
 * <li>Description: TODO</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2019-06-13 23:06</li>
 * <li>Version: V1.0</li>
 */
public class GetVertx {
    public static Vertx getTx(){

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(10);
        vo.setWorkerPoolSize(20);
        return Vertx.vertx(vo);
    }
}
