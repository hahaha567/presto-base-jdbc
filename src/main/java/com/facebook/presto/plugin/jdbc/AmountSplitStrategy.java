package com.facebook.presto.plugin.jdbc;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @Author hahaha567
 * @Date 2023/10/9 20:38
 */
public class AmountSplitStrategy implements SplitStrategy {

    private Long amount = 100000L;

    public AmountSplitStrategy() {}

    public AmountSplitStrategy(Long amount) {
        this.amount = amount;
    }

    @Override
    public List<JdbcSplit> getTableSplits(String connectorId, JdbcIdentity identity, JdbcTableLayoutHandle layoutHandle, JdbcTableHandle tableHandle, Long rowsNum) {
        List<JdbcSplit> jdbcSplits = new ArrayList<>();
        Long start = 0L;
        while (start + amount < rowsNum) {
            jdbcSplits.add(new JdbcSplit(
                    connectorId,
                    tableHandle.getCatalogName(),
                    tableHandle.getSchemaName(),
                    tableHandle.getTableName(),
                    layoutHandle.getTupleDomain(),
                    layoutHandle.getAdditionalPredicate(),
                    start,
                    amount
            ));
            start += amount;
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
