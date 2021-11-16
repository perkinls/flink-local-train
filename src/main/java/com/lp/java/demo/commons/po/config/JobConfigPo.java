package com.lp.java.demo.commons.po.config;

import java.io.Serializable;

/**
 * @author li.pan
 * @version 1.0.0
 * @Description 当前Job配置类
 * @createTime 2021年01月22日 15:06:00
 */
public class JobConfigPo implements Serializable {

    private static final long serialVersionUID = 7337953085292522601L;
    /**
     * 任务运行并行度
     */
    public static Integer defaultParallelism;

    /**
     * 任务运行并行度
     */
    public static Integer sinkParallelism;

    /**
     * 任务名前缀
     */
    public static String jobNamePrefix;


    /**
     * 是否开启checkpoint
     */
    public static Boolean enableCheckpoint;

    /**
     * checkpoint时间间隔
     */
    public static Long checkpointInterval;

    /**
     * checkpoint类型
     */
    public static String checkpointStateType;

    /**
     * checkpoint路径
     */
    public static String checkPointPath;

    /**
     * checkpoint超时时间
     */
    public static Long checkpointTimeOut;

    /**
     * 两次CheckPoint中间最小时间间隔
     */
    public static Long checkpointMinPauseBetween;

    /**
     * 同时允许多少个checkpoint做快照
     */
    public static Integer checkpointCurrentCheckpoints;

    /**
     * 默认使用FixedDelayRestartStrategy重启策略的重试次数(3次)
     */
    public static Integer checkpointRestartAttempts;

    /**
     * 默认使用FixedDelayRestartStrategy重启策略的的每次重启时间间隔
     */
    public static Long checkpointRestartAttemptsInterval;

}
