package com.damai.shardingsphere;

import cn.hutool.core.collection.CollectionUtil;
import com.damai.enums.BaseCode;
import com.damai.exception.DaMaiFrameException;
import com.github.pagehelper.util.StringUtil;
import org.apache.shardingsphere.sharding.api.sharding.complex.ComplexKeysShardingAlgorithm;
import org.apache.shardingsphere.sharding.api.sharding.complex.ComplexKeysShardingValue;

import java.util.*;

public class DatabaseOrderComplexGeneArithmetic implements ComplexKeysShardingAlgorithm <Long> {
    private static final String SHARDING_COUNT_KEY_NAME = "sharding-count";

    private static final String TABLE_SHARDING_COUNT_KEY_NAME = "table-sharding-count";

    private int shardingCount;

    private int tableShardingCount;

    @Override
    public void init(Properties props) {
        this.shardingCount = Integer.parseInt(props.getProperty(SHARDING_COUNT_KEY_NAME));
        this.tableShardingCount = Integer.parseInt(props.getProperty(TABLE_SHARDING_COUNT_KEY_NAME));
    }

    @Override
    public Collection<String> doSharding(Collection<String> allActualSplitDatabaseNames, ComplexKeysShardingValue<Long> complexKeysShardingValue) {
        List<String> actualDatabaseNames = new ArrayList<>(allActualSplitDatabaseNames.size());
        // 获取分片键值
        Map<String, Collection<Long>> columnNameAndShardingValueMap = complexKeysShardingValue.getColumnNameAndShardingValuesMap();

        if(CollectionUtil.isEmpty(columnNameAndShardingValueMap)){
            return actualDatabaseNames;
        }

        Collection<Long> orderNumberValues = columnNameAndShardingValueMap.get("order_number");
        Collection<Long> userIdValues = columnNameAndShardingValueMap.get("user_id");

        Long value = null;

        if(CollectionUtil.isNotEmpty(orderNumberValues)){
            value = orderNumberValues.stream().findFirst().orElseThrow(() -> new DaMaiFrameException(BaseCode.ORDER_NUMBER_NOT_EXIST));
        }
        if(CollectionUtil.isNotEmpty(userIdValues)){
            value = userIdValues.stream().findFirst().orElseThrow(() -> new DaMaiFrameException(BaseCode.ORDER_NUMBER_NOT_EXIST));
        }
        if(Objects.nonNull(value)){
            long databaseIndex = calculateDatabaseIndex(shardingCount, value, tableShardingCount);
            String databaseIndexStr = String.valueOf(databaseIndex);
            for (String actualSplitDatabaseName : allActualSplitDatabaseNames) {
                if(actualSplitDatabaseName.contains(databaseIndexStr)){
                    actualDatabaseNames.add(actualSplitDatabaseName);
                    break;
                }
            }
            return actualDatabaseNames;
        }
        else{
            return allActualSplitDatabaseNames;
        }

    }

    public long calculateDatabaseIndex(Integer databaseCount, Long splicingKey, Integer tableCount) {
        String splicingKeyBinary = Long.toBinaryString(splicingKey);
        long replacementLength = log2N(tableCount);
        String geneBinaryStr = splicingKeyBinary.substring(splicingKeyBinary.length() - (int) replacementLength);

        if(StringUtil.isNotEmpty(geneBinaryStr)){
            int h;
            int geneOptimizeHashCode = (h = geneBinaryStr.hashCode()) ^ (h >>> 16);
            return geneOptimizeHashCode & (databaseCount - 1);
        }
        throw new DaMaiFrameException(BaseCode.NOT_FOUND_GENE);
    }

    public long log2N(long count) {
        return (long) (Math.log(count) / Math.log(2));
    }
}
