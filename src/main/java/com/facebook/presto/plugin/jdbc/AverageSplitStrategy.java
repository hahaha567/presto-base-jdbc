package com.facebook.presto.plugin.jdbc;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @Author hahaha567
 * @Date 2023/10/9 20:45
 */
public class AverageSplitStrategy implements SplitStrategy {

    private Integer count = 5;

    public AverageSplitStrategy() {}

    public AverageSplitStrategy(Integer count) {
        this.count = count;
    }

    @Override
    public List<JdbcSplit> getTableSplits(String connectorId, JdbcIdentity identity, JdbcTableLayoutHandle layoutHandle, JdbcTableHandle tableHandle, Long rowsNum) {
        List<JdbcSplit> jdbcSplits = new ArrayList<>();
        Long start = 0L, length = rowsNum / count;
        for (int i = 0; i < count - 1; i++) {
            jdbcSplits.add(new JdbcSplit(
                    connectorId,
                    tableHandle.getCatalogName(),
                    tableHandle.getSchemaName(),
                    tableHandle.getTableName(),
                    layoutHandle.getTupleDomain(),
                    layoutHandle.getAdditionalPredicate(),
                    start,
                    length
            ));
            start += length;
        }
        if (start < rowsNum) {
            jdbcSplits.add(new JdbcSplit(
                    connectorId,
                    tableHandle.getCatalogName(),
                    tableHandle.getSchemaName(),
                    tableHandle.getTableName(),
                    layoutHandle.getTupleDomain(),
                    layoutHandle.getAdditionalPredicate(),
                    start,
                    rowsNum - start
            ));
        }
        return jdbcSplits;
    }
}
