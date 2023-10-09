package com.facebook.presto.plugin.jdbc;

import java.util.List;

/**
 * @Description
 * @Author hahaha567
 * @Date 2023/10/9 20:36
 */
public interface SplitStrategy {
    List<JdbcSplit> getTableSplits(String connectorId, JdbcIdentity identity, JdbcTableLayoutHandle layoutHandle,
                                   JdbcTableHandle tableHandle, Long rowsNum);
}
