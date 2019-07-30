package org.dbist.ddl;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import net.sf.common.util.ValueUtils;

@Service
public class DdlStartUpService {

    @Resource
    public Environment env;

    @Autowired(required = false)
    private Ddl ddl;

    @Autowired(required = false)
    private InitialSetup initialSetup;

    @EventListener({ContextRefreshedEvent.class})
    public void start() {
        // 1. Table Space Setup
        String dataTBSpace = ValueUtils.toString(env.getProperty("dbist.ddl.tablespace.data", ""));
        String idxTBSpace = ValueUtils.toString(env.getProperty("dbist.ddl.tablespace.idx", ""));
        this.ddl.setTableSpace(dataTBSpace, idxTBSpace);

        // 2. initial data setup
        if (initialSetup != null)
            this.initialSetup.initialSetup(this.env);
    }
}