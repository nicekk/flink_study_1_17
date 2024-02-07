package com.kkarch.sink;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.RandomUtil;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.io.File;

/**
 * 为了实现一个sink到file的功能
 * 并且实现两阶段提交
 * 预提交的时候，先写入到临时文件中；
 * 真正提交的时候，再从临时文件读取到最终的文件中。
 *
 * 限制并发度为1
 *
 * @author wangkai
 * @date 2024/2/7 21:30
 **/
public class TwoPhaseFileSink extends TwoPhaseCommitSinkFunction<Tuple2<String,Integer>,TwoPhaseFileSink.FileState,Void> {

    private static final String TARGET_FILE_NAME = "d://flink//sink//target.txt";

    public TwoPhaseFileSink() {
        super(new KryoSerializer<>(TwoPhaseFileSink.FileState.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    @Override
    protected void invoke(FileState transaction, Tuple2<String, Integer> value, Context context) throws Exception {
        // 写入临时文件
        String tmpFileName = transaction.tempFile;
        String content = value.f0 + " " + value.f1 + "\n";
        FileUtil.writeString(content, tmpFileName, "utf-8");
    }

    @Override
    protected FileState beginTransaction() throws Exception {
        // 开启事务的时候，要先创建一个临时文件,并返回
        String tmpFileName = "d://flink//sink//tmp" + RandomUtil.randomString(5) + ".txt";
        return new FileState(tmpFileName,TARGET_FILE_NAME);
    }

    @Override
    protected void preCommit(FileState transaction) throws Exception {

    }

    @Override
    protected void commit(FileState transaction) {
        // 正式提交的时候，从临时文件读取内容，并追加到最终文件中
        String tmpFileName = transaction.tempFile;
        File file = new File(tmpFileName);
        if (!file.exists()) {
            return;
        }
        String content = FileUtil.readString(tmpFileName, "utf-8");
        FileUtil.appendString(content, transaction.finalFile, "utf-8");

        // 删除临时文件
        FileUtil.del(tmpFileName);
    }

    @Override
    protected void abort(FileState transaction) {
        // 事务失败的时候，删除临时文件
        FileUtil.del(transaction.tempFile);
    }

    public static class FileState {
        private transient String tempFile;
        private transient String finalFile;

        public FileState(String tempFile, String finalFile) {
            this.tempFile = tempFile;
            this.finalFile = finalFile;
        }
    }
}
