package com.kkarch.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Random;

/**
 * @author wangkai
 * @date 2024/2/7 8:06
 **/
public class TwoPhaseMysqlSink extends TwoPhaseCommitSinkFunction<Tuple2<String, Integer>, TwoPhaseMysqlSink.ConnectionState, Void> {

    private Random random = new Random();

    private static final Logger log = LoggerFactory.getLogger(TwoPhaseMysqlSink.class);

    public TwoPhaseMysqlSink() {
        super(new KryoSerializer<>(TwoPhaseMysqlSink.ConnectionState.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
        log.warn("MysqlTwoPhaseSink init...");
    }

    /**
     * 执行数据库入库操作  task初始化的时候调用
     *
     * @param tp
     * @param context
     * @throws Exception
     */
    @Override
    protected void invoke(TwoPhaseMysqlSink.ConnectionState connectionState, Tuple2<String, Integer> tp, Context context) throws Exception {
        log.warn("start invoke...");
        Connection connection = connectionState.connection;
        String value = tp.f0;
        Integer total = tp.f1;
        String sql = "insert into `t_test` (`value`,`total`,`insert_time`) values (?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, value);
        ps.setInt(2, total);
        ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
        if (ps != null) {
            String sqlStr = ps.toString().substring(ps.toString().indexOf(":") + 2);
            log.info("执行的SQL语句:{}", sqlStr);
        }
        //执行insert语句
        ps.execute();
        //手动制造异常
        if ("Errors".equals(value)) {
            if (random.nextInt(3) == 1) {
                log.error("1/3的概率发生了异常");
                System.out.println(1 / 0);
            }
        }
    }

    /**
     * 获取连接，开启手动提交事物（getConnection方法中）
     *
     * @return
     * @throws Exception
     */
    @Override
    protected TwoPhaseMysqlSink.ConnectionState beginTransaction() throws Exception {
        log.warn("start beginTransaction...");
        Connection connection = DBConnectUtil.getConnection();
        connection.setAutoCommit(false);
        return new TwoPhaseMysqlSink.ConnectionState(connection);
    }

    /**
     *
     * @param connection
     * @throws Exception
     */
    @Override
    protected void preCommit(TwoPhaseMysqlSink.ConnectionState connection) throws Exception {
        log.warn("start preCommit...");
    }

    /**
     * 如果invoke方法执行正常，则提交事务
     */
    @Override
    protected void commit(TwoPhaseMysqlSink.ConnectionState connectionState) {
        log.warn("start commit...");
        DBConnectUtil.commit(connectionState.connection);
    }

    /**
     * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
     */
    @Override
    protected void abort(TwoPhaseMysqlSink.ConnectionState connectionState) {
        log.warn("start abort rollback...");
        DBConnectUtil.rollback(connectionState.connection);
    }

    public static class ConnectionState {
        private final transient Connection connection;

        ConnectionState(Connection connection) {
            this.connection = connection;
        }
    }
}

