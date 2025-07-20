package com.damai.service.init;

import com.damai.core.SpringUtil;
import com.damai.initialize.base.AbstractApplicationPostConstructHandler;
import com.damai.service.ProgramService;
import com.damai.service.ProgramShowTimeService;
import com.damai.util.BusinessEsHandle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
public class ProgramShowTimeRenewal extends AbstractApplicationPostConstructHandler {

    @Autowired
    private BusinessEsHandle businessEsHandle;

    @Autowired
    private ProgramShowTimeService programShowTimeService;

    @Autowired
    private ProgramService programService;


    @Override
    public Integer executeOrder() {
        return 2;
    }

    @Override
    public void executeInit(ConfigurableApplicationContext context) {
        Set<Long> programIdSet = programShowTimeService.renewal();
        if(!programIdSet.isEmpty()) {
            businessEsHandle.deleteIndex(SpringUtil.getPrefixDistinctionName() + "-" +
                    ProgramDocumentParamName.INDEX_NAME);
            for (Long programId : programIdSet) {
                programService.delRedisData(programId);
                programService.delLocalCache(programId);
            }
        }
    }
}
