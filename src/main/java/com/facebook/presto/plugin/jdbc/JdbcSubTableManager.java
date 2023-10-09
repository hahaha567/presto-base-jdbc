package com.facebook.presto.plugin.jdbc;

import java.util.List;

/**
 * @Description
 * @Author hahaha567
 * @Date 2023/10/9 20:20
 */
public class JdbcSubTableManager {
    private static final Long DATA_COUNT = 500000L;

    public static List<JdbcSplit> getTableSplits(String connectorId, JdbcIdentity identity, JdbcTableLayoutHandle layoutHandle,
                                          JdbcTableHandle tableHandle, Long rowsNum) {
        SplitStrategy splitStrategy = rowsNum < DATA_COUNT ? new AverageSplitStrategy() : new AmountSplitStrategy();
        return splitStrategy.getTableSplits(connectorId, identity, layoutHandle, tableHandle, rowsNum);
    }
}
