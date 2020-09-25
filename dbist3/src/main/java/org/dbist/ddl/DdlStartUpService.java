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

    @EventListener({ContextRefreshedEvent.class})
    public void start() {
        String ddlEnable = env.getProperty("dbist.ddl.enable", "false");
        if (ValueUtils.toBoolean(ddlEnable))
            ddl.createTable();
    }
}