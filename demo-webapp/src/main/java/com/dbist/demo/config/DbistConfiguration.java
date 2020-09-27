package com.dbist.demo.config;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.dbist.aspect.SqlAspect;
import org.dbist.ddl.impl.DdlJdbc;
import org.dbist.dml.impl.DmlJdbc;
import org.dbist.processor.impl.VelocityPreprocessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

@Configuration
public class DbistConfiguration {

    @Resource
    private Environment env;

    @Bean
    public VelocityPreprocessor velocityPreprocessor() {
        return new VelocityPreprocessor();
    }

    @Bean
    public DmlJdbc dml(DataSource dataSource,
                       JdbcOperations jdbcOperations,
                       NamedParameterJdbcTemplate namedParameterJdbcTemplate,
                       VelocityPreprocessor velocityPreprocessor) {
        return new DmlJdbc() {{
            this.setDomain(env.getProperty("dml.domain", "public"));
            this.setDataSource(dataSource);
            this.setJdbcOperations(jdbcOperations);
            this.setNamedParameterJdbcOperations(namedParameterJdbcTemplate);
            this.setPreprocessor(velocityPreprocessor);
        }};
    }

    @Bean
    public DdlJdbc ddl() {
        return new DdlJdbc();
    }

    @Bean
    public SqlAspect sqlAspect() {
        return new SqlAspect() {{
            String enabled = env.getProperty("sqlAspect.enabled", "false");
            String prettyPrint = env.getProperty("sqlAspect.prettyPrint", "false");
            String combinedPrint = env.getProperty("sqlAspect.combinedPrint", "false");
            String includeElapsedTime = env.getProperty("sqlAspect.includeElapsedTime", "false");

            this.setEnabled(enabled);
            this.setPrettyPrint(prettyPrint);
            this.setCombinedPrint(combinedPrint);
            this.setIncludeElapsedTime(includeElapsedTime);
        }};
    }
}
