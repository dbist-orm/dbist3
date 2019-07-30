/**
 * Copyright 2011-2014 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dbist.dml.jdbc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dbist.dml.Lock;
import org.dbist.metadata.Sequence;

/**
 * @author Steve M. Jung
 * @since 2013. 9. 7. (version 2.0.3)
 */
public abstract class AbstractQueryMapper implements QueryMapper {

    private String reservedWordEscapingRule = "uppercase";

    public void setReservedWordEscapingRule(String reservedWordEscapingRule) {
        this.reservedWordEscapingRule = reservedWordEscapingRule;
    }

    private Map<String, String> reservedWordMap;

    public String toReservedWordEscapedName(String name) {
        if (reservedWordMap == null) {
            synchronized (this) {
                if (reservedWordMap == null) {
                    Map<String, String> map = new HashMap<String, String>();
                    if ("uppercase".equals(reservedWordEscapingRule)) {
                        for (String word : getReservedWords())
                            map.put(word, getReservedWordEscapingBraceOpen() + word.toUpperCase() + getReservedWordEscapingBraceClose());
                    } else {
                        for (String word : getReservedWords())
                            map.put(word, getReservedWordEscapingBraceOpen() + word + getReservedWordEscapingBraceClose());
                    }
                    reservedWordMap = map;
                }
            }
        }

        return reservedWordMap.containsKey(name) ? reservedWordMap.get(name) : name;
    }

    public String toNextval(Sequence sequence) {
        if (sequence.getName() == null || sequence.isAutoIncrement())
            return null;
        return sequence.getDomain() + "." + sequence.getName() + ".nextval";
    }

    public String toEscapement(char escape) {
        return "escape '" + escape + "'";
    }

    public String toWithLock(Lock lock) {
        return null;
    }

    public String toForUpdate(Lock lock) {
        StringBuffer buf = new StringBuffer();
        buf.append("for update");

        if (!isSupportedLockTimeout())
            return buf.toString();

        Integer timeout = lock.getTimeout();
        if (timeout == null || timeout < 0)
            return buf.toString();

        timeout /= 1000;
        if (timeout == 0)
            buf.append(" nowait");
        else
            buf.append(" wait " + timeout);
        return buf.toString();
    }

    private Set<String> reservedWords;

    public Set<String> getReservedWords() {
        if (reservedWords == null) {
            synchronized (this) {
                if (reservedWords == null) {
                    Set<String> set = new HashSet<String>();

                    set.add("absolute");

                    // oracle
                    set.add("access");
                    // mysql
                    set.add("accessible");
                    // oracle
                    set.add("account");

                    set.add("action");

                    set.add("add");
                    // oracle
                    set.add("admin");
                    // oracle
                    set.add("advice");

                    set.add("after");

                    set.add("all");
                    // oracle
                    set.add("all_rows");

                    set.add("allocate");

                    set.add("alter");
                    // postgresql
                    set.add("analyze");
                    // mysql, postgresql, oracle
                    set.add("analyze");

                    set.add("and");

                    set.add("any");
                    // oracle
                    set.add("archive");
                    // oracle
                    set.add("archivelog");

                    set.add("are");

                    set.add("array");

                    set.add("as");

                    set.add("asc");

                    set.add("asensitive");

                    set.add("assertion");
                    // db2
                    set.add("associate");
                    // db2
                    set.add("asutime");

                    set.add("asymmetric");
                    // oracle
                    set.add("at");

                    set.add("atomic");
                    // oracle, db2
                    set.add("audit");
                    // oracle
                    set.add("authenticated");

                    set.add("authorization");
                    // oracle
                    set.add("autoextend");
                    // oracle
                    set.add("automatic");
                    // db2
                    set.add("aux");
                    // db2
                    set.add("auxiliary");

                    set.add("avg");
                    // oracle, sqlserver
                    set.add("backup");
                    // oracle
                    set.add("become");

                    set.add("before");

                    set.add("begin");

                    set.add("between");
                    // oracle
                    set.add("bfile");

                    set.add("bigint");

                    set.add("binary");

                    set.add("bit");
                    // oracle
                    set.add("bitmap");

                    set.add("bitlength");

                    set.add("blob");
                    // oracle
                    set.add("block");
                    // oracle
                    set.add("body");

                    set.add("boolean");

                    set.add("both");
                    // sqlserver
                    set.add("break");

                    set.add("breath");
                    // sqlserver
                    set.add("browse");
                    // db2
                    set.add("bufferpool");
                    // sqlserver
                    set.add("bulk");

                    set.add("by");

                    set.add("call");
                    // oracle
                    set.add("cache");
                    // oracle
                    set.add("cache_instances");
                    // mysql
                    set.add("call");
                    // oracle
                    set.add("cancel");
                    // db2
                    set.add("capture");

                    set.add("cascade");

                    set.add("cascaded");

                    set.add("case");

                    set.add("cast");

                    set.add("catalog");
                    // db2
                    set.add("ccsid");
                    // oracle
                    set.add("cfile");
                    // oracle
                    set.add("chained");
                    // mysql, oracle
                    set.add("change");

                    set.add("char");
                    // oracle
                    set.add("char_cs");

                    set.add("char_length");

                    set.add("character");

                    set.add("character_length");

                    set.add("check");
                    // oracle, sqlserver
                    set.add("checkpoint");
                    // oracle
                    set.add("choose");
                    // oracle
                    set.add("chunk");
                    // oracle
                    set.add("clear");

                    set.add("clob");
                    // oracle
                    set.add("clone");

                    set.add("close");
                    // oracle
                    set.add("close_cached_open_cursors");
                    // oracle
                    set.add("cluster");
                    // sqlserver
                    set.add("clustered");

                    set.add("coalesce");

                    set.add("collate");

                    set.add("collation");
                    // db2
                    set.add("collection");
                    // db2
                    set.add("collid");

                    set.add("column");
                    // oracle
                    set.add("columns");
                    // oracle, db2
                    set.add("comment");

                    set.add("commit");
                    // oracle
                    set.add("commited");
                    // oracle
                    set.add("compatibility");
                    // oracle
                    set.add("compile");
                    // oracle
                    set.add("complete");
                    // oracle
                    set.add("composit_limit");
                    // oracle
                    set.add("compress");
                    // oracle, sqlserver
                    set.add("compute");
                    // db2
                    set.add("concat");

                    set.add("condition");

                    set.add("connect");

                    set.add("connection");
                    // oracle
                    set.add("connect_time");

                    set.add("constraint");

                    set.add("constraints");

                    set.add("constructure");

                    set.add("contains");
                    // sqlserver
                    set.add("containstable");
                    // db2
                    set.add("content");
                    // oracle
                    set.add("contents");

                    set.add("continue");
                    // oracle
                    set.add("controlfile");

                    set.add("convert");

                    set.add("corresponding");
                    // oracle
                    set.add("cost");

                    set.add("count");
                    // oracle
                    set.add("cpu_per_call");
                    // oracle
                    set.add("cpu_per_session");

                    set.add("create");

                    set.add("cross");

                    set.add("cube");
                    // oracle, sqlserver, db2
                    set.add("current");

                    set.add("current_date");

                    set.add("current_default_transform_group");
                    // db2
                    set.add("current_lc_ctype");

                    set.add("current_path");

                    set.add("current_role");
                    // oracle
                    set.add("current_schema");

                    set.add("current_time");

                    set.add("current_timestamp");

                    set.add("current_transform_group_for_type");

                    set.add("current_user");

                    set.add("cursor");

                    set.add("cycle");
                    // oracle
                    set.add("dangling");

                    set.add("data");
                    // mysql, oracle, sqlserver
                    set.add("database");
                    // mysql
                    set.add("databases");
                    // oracle
                    set.add("datafile");
                    // oracle
                    set.add("dataobjno");

                    set.add("date");

                    set.add("day");
                    // db2
                    set.add("days");
                    // mysql
                    set.add("day_hour");
                    // mysql
                    set.add("day_microsecond");
                    // mysql
                    set.add("day_minute");
                    // mysql
                    set.add("day_second");
                    // oracle
                    set.add("dba");
                    // sqlserver
                    set.add("dbcc");
                    // oracle
                    set.add("dbhigh");
                    // db2
                    set.add("dbinfo");
                    // oracle
                    set.add("dblow");
                    // oracle
                    set.add("dbmac");

                    set.add("deallocate");
                    // oracle
                    set.add("debug");

                    set.add("dec");

                    set.add("decimal");

                    set.add("declare");

                    set.add("default");

                    set.add("deferrable");

                    set.add("deferred");
                    // oracle
                    set.add("degree");
                    // mysql
                    set.add("delayed");

                    set.add("delete");
                    // sqlserver
                    set.add("deny");

                    set.add("depth");

                    set.add("deref");

                    set.add("desc");

                    set.add("describe");

                    set.add("descriptor");

                    set.add("deterministic");

                    set.add("diagnostics");

                    set.add("disconnect");

                    // oracle
                    set.add("directory");
                    // oracle, db2
                    set.add("disable");
                    // db2
                    set.add("disallow");
                    // oracle
                    set.add("disconnect");
                    // sqlserver
                    set.add("disk");
                    // oracle
                    set.add("dismount");

                    set.add("distinct");
                    // mysql
                    set.add("distinctrow");
                    // oracle, sqlserver
                    set.add("distributed");
                    // mysql
                    set.add("div");
                    // oracle
                    set.add("dml");

                    set.add("do");
                    // db2
                    set.add("document");

                    set.add("domain");

                    set.add("double");

                    set.add("drop");

                    set.add("dynamic");
                    // mysql
                    set.add("dual");
                    // sqlserver
                    set.add("dummy");
                    // oracle, sqlserver
                    set.add("dump");

                    set.add("each");
                    // db2
                    set.add("editproc");

                    set.add("else");

                    set.add("elseif");
                    // oracle
                    set.add("enable");
                    // mysql
                    set.add("enclosed");
                    // db2
                    set.add("encoding");
                    // db2
                    set.add("encryption");

                    set.add("end");
                    // db2
                    set.add("ending");

                    set.add("equals");
                    // oracle
                    set.add("enforce");
                    // oracle
                    set.add("entry");
                    // db2
                    set.add("erase");
                    // sqlserver
                    set.add("errlvl");

                    set.add("escape");
                    // mysql
                    set.add("escaped");

                    set.add("except");

                    set.add("exception");
                    // oracle
                    set.add("exceptions");
                    // oracle
                    set.add("exchange");
                    // oracle
                    set.add("excluding");
                    // oracle
                    set.add("exclusive");

                    set.add("exec");

                    set.add("execute");

                    set.add("exists");

                    set.add("exit");

                    set.add("external");

                    set.add("extract");
                    // oracle
                    set.add("expire");
                    // mysql
                    set.add("explain");
                    // oracle
                    set.add("extent");
                    // oracle
                    set.add("extents");
                    // db2
                    set.add("external");
                    // oracle
                    set.add("externally");
                    // oracle
                    set.add("failed_login_attempts");

                    set.add("false");
                    // oracle
                    set.add("fast");
                    // db2
                    set.add("fenced");

                    set.add("fetch");
                    // db2
                    set.add("fieldproc");
                    // oracle, sqlserver
                    set.add("file");
                    // sqlserver
                    set.add("filfactor");

                    set.add("filter");
                    // db2
                    set.add("final");

                    set.add("first");
                    // oracle
                    set.add("first_rows");
                    // oracle
                    set.add("flagger");

                    set.add("float");
                    // mysql
                    set.add("float4");
                    // mysql
                    set.add("float8");
                    // oracle
                    set.add("flob");
                    // oracle
                    set.add("flush");

                    set.add("for");
                    // mysql, oracle
                    set.add("force");

                    set.add("foreign");

                    set.add("found");

                    set.add("free");
                    // oracle
                    set.add("freelist");
                    // oracle
                    set.add("freelists");
                    // sqlserver
                    set.add("freetext");
                    // sqlserver
                    set.add("freetextable");

                    set.add("from");

                    set.add("full");
                    // mysql
                    set.add("fulltext");

                    set.add("function");

                    set.add("general");
                    // db2
                    set.add("generated");

                    set.add("get");

                    set.add("global");
                    // oracle
                    set.add("globallys");
                    // oracle
                    set.add("global_name");

                    set.add("go");

                    set.add("goto");

                    set.add("grant");

                    set.add("group");

                    set.add("grouping");
                    // oracle
                    set.add("groups");

                    set.add("handler");
                    // oracle
                    set.add("hash");
                    // oracle
                    set.add("hashkeys");

                    set.add("having");
                    // oracle
                    set.add("header");
                    // oracle
                    set.add("heap");
                    // mysql
                    set.add("high_priority");

                    set.add("hold");
                    // sqlserver
                    set.add("holdlock");

                    set.add("hour");
                    // db2
                    set.add("hours");
                    // mysql
                    set.add("hour_microsecond");
                    // mysql
                    set.add("hour_minute");
                    // mysql
                    set.add("hour_second");
                    // oracle
                    set.add("identified");

                    set.add("identity");
                    // sqlserver
                    set.add("identity_insert");
                    // sqlserver
                    set.add("identitycol");
                    // oracle
                    set.add("idgenerators");
                    // oracle
                    set.add("idle_time");
                    // postgresql
                    set.add("ilike");

                    set.add("if");
                    // mysql
                    set.add("ignore");

                    set.add("immediate");

                    set.add("in");
                    // oracle
                    set.add("including");
                    // db2
                    set.add("inclusive");
                    // oracle
                    set.add("increment");
                    // mysql, oracle, sqlserver, db2
                    set.add("index");
                    // oracle
                    set.add("indexed");
                    // oracle
                    set.add("indexes");

                    set.add("indicator");
                    // oracle
                    set.add("ind_partition");
                    // db2
                    set.add("inherit");
                    // oracle
                    set.add("initial");

                    set.add("initially");
                    // oracle
                    set.add("initrans");
                    // mysql
                    set.add("infile");

                    set.add("inner");

                    set.add("inout");

                    set.add("input");

                    set.add("insensitive");

                    set.add("insert");
                    // oracle
                    set.add("instance");
                    // oracle
                    set.add("instances");
                    // oracle
                    set.add("instead");

                    set.add("int");
                    // mysql
                    set.add("int1");
                    // mysql
                    set.add("int2");
                    // mysql
                    set.add("int3");
                    // mysql
                    set.add("int4");
                    // mysql
                    set.add("int8");

                    set.add("integer");
                    // oracle
                    set.add("intermediate");

                    set.add("intersect");

                    set.add("interval");

                    set.add("into");
                    // mysql
                    set.add("io_after_gtids");
                    // mysql
                    set.add("io_before_gtids");

                    set.add("is");
                    // postgresql
                    set.add("isnull");
                    // db2
                    set.add("isobid");

                    set.add("isolation");
                    // oracle
                    set.add("isolation_level");

                    set.add("iterate");
                    // db2
                    set.add("jar");

                    set.add("join");
                    // oracle
                    set.add("keep");

                    set.add("key");
                    // mysql
                    set.add("keys");
                    // mysql, oracle, sqlserver
                    set.add("kill");
                    // oracle
                    set.add("label");

                    set.add("language");

                    set.add("large");

                    set.add("last");
                    // oracle
                    set.add("layer");
                    // db2
                    set.add("lc_ctype");

                    set.add("leading");

                    set.add("leave");

                    set.add("left");
                    // oracle
                    set.add("less");

                    set.add("level");
                    // oracle
                    set.add("library");

                    set.add("like");
                    // mysql, oracle
                    set.add("limit");
                    //mysql
                    set.add("linear");
                    // sqlserver
                    set.add("lineno");
                    // mysql
                    set.add("lines");
                    // oracle
                    set.add("link");
                    // oracle
                    set.add("list");
                    // mysql, sqlserver
                    set.add("load");
                    // oracle
                    set.add("lob");

                    set.add("local");
                    // db2
                    set.add("locale");

                    set.add("localtime");

                    set.add("localtimestamp");

                    set.add("locator");
                    // db2
                    set.add("locators");
                    // mysql, oracle
                    set.add("lock");
                    // oracle
                    set.add("locked");
                    // db2
                    set.add("lockmax");
                    // db2
                    set.add("locksize");
                    // oracle
                    set.add("log");
                    // oracle
                    set.add("logfile");
                    // oracle
                    set.add("logging");
                    // oracle
                    set.add("logical_reads_per_call");
                    // oracle
                    set.add("logical_reads_per_session");
                    // mysql, oracle, db2
                    set.add("long");
                    // mysql
                    set.add("longlob");
                    // mysql
                    set.add("longtext");

                    set.add("loop");

                    set.add("lower");
                    // mysql
                    set.add("low_priority");
                    // db2
                    set.add("maintained");
                    // oracle
                    set.add("manage");

                    set.add("map");
                    // oracle
                    set.add("master");
                    // mysql
                    set.add("master_bind");
                    // mysql
                    set.add("master_ssl_verify_server_cert");

                    set.add("match");
                    // db2
                    set.add("materialized");

                    set.add("max");
                    // oracle
                    set.add("maxarchlogs");
                    // oracle
                    set.add("maxdatafiles");
                    // oracle
                    set.add("maxextents");
                    // oracle
                    set.add("maxinstances");
                    // oracle
                    set.add("maxlogfiles");
                    // oracle
                    set.add("maxloghistory");
                    // oracle
                    set.add("maxlogmembers");
                    // oracle
                    set.add("maxsize");
                    // oracle
                    set.add("maxtrans");
                    // mysql, oracle
                    set.add("maxvalue");
                    // mysql
                    set.add("mediumblob");
                    // mysql
                    set.add("mediumint");
                    // mysql
                    set.add("mediumtext");

                    set.add("member");

                    set.add("merge");

                    set.add("method");
                    // db2
                    set.add("microsecond");
                    // db2
                    set.add("microseconds");
                    // mysql
                    set.add("middleint");

                    set.add("min");
                    // oracle
                    set.add("minimum");
                    // oracle
                    set.add("minextents");
                    // oracle
                    set.add("minus");

                    set.add("minute");
                    // db2
                    set.add("minutes");
                    // mysql
                    set.add("minute_microsecond");
                    // mysql
                    set.add("minute_second");
                    // oracle
                    set.add("minvalue");
                    // oracle
                    set.add("mlslabel");
                    // oracle
                    set.add("mls_label_format");
                    // mysql
                    set.add("mod");
                    // oracle
                    set.add("mode");

                    set.add("modifies");
                    // oracle
                    set.add("modify");

                    set.add("module");

                    set.add("month");
                    // db2
                    set.add("months");
                    // oracle
                    set.add("mount");
                    // oracle
                    set.add("move");
                    // oracle
                    set.add("mts_dispatchers");
                    // oracle
                    set.add("multiset");

                    set.add("names");

                    set.add("national");

                    set.add("natural");

                    set.add("nchar");
                    // oracle
                    set.add("nchar_cs");

                    set.add("nclob");
                    // oracle
                    set.add("needed");
                    // oracle
                    set.add("nested");
                    // oracle
                    set.add("network");

                    set.add("new");

                    set.add("next");
                    // db2
                    set.add("nextval");

                    set.add("no");
                    // oracle
                    set.add("noarchivelog");
                    // oracle
                    set.add("noaudit");
                    // oracle
                    set.add("nocache");
                    // sqlserver
                    set.add("nocheck");
                    // oracle
                    set.add("nocompress");
                    // oracle
                    set.add("nocycle");
                    // oracle
                    set.add("noforce");
                    // oracle
                    set.add("nologging");
                    // oracle
                    set.add("nomaxvalue");
                    // oracle
                    set.add("nominvalue");
                    // sqlserver
                    set.add("nonclustered");

                    set.add("none");
                    // oracle
                    set.add("noorder");
                    // oracle
                    set.add("nooverride");
                    // oracle
                    set.add("noparallel");
                    // oracle
                    set.add("noreverse");
                    // oracle
                    set.add("normal");
                    // oracle
                    set.add("nosort");

                    set.add("not");
                    // oracle
                    set.add("nothing");
                    // oracle
                    set.add("nowait");
                    // mysql
                    set.add("no_write_to_binlog");

                    set.add("null");

                    set.add("nullif");
                    // db2
                    set.add("nulls");
                    // oracle
                    set.add("number");

                    set.add("numeric");
                    // db2
                    set.add("numparts");
                    // oracle
                    set.add("nvarchar2");
                    // db2
                    set.add("obid");

                    set.add("object");
                    // oracle
                    set.add("objno");
                    // oracle
                    set.add("objno_reuse");

                    set.add("octet_length");

                    set.add("of");
                    // oracle, sqlserver
                    set.add("off");
                    // oracle
                    set.add("offline");
                    // sqlserver
                    set.add("offsets");
                    // oracle
                    set.add("oid");
                    // oracle
                    set.add("oidindex");

                    set.add("old");

                    set.add("on");
                    // oracle
                    set.add("online");

                    set.add("only");
                    // oracle
                    set.add("opcode");

                    set.add("open");
                    // sqlserver
                    set.add("opendatasource");
                    // sqlserver
                    set.add("openquery");
                    // sqlserver
                    set.add("openrowset");
                    // sqlserver
                    set.add("openxml");
                    // oracle
                    set.add("optimal");
                    // db2
                    set.add("optimization");
                    // mysql, db2
                    set.add("optimize");
                    // oracle
                    set.add("optimizer_goal");

                    set.add("option");
                    // mysql
                    set.add("optionally");

                    set.add("or");

                    set.add("order");

                    set.add("ordinality");
                    // oracle, db2
                    set.add("organization");
                    // oracle
                    set.add("oslabel");

                    set.add("out");

                    set.add("outer");

                    set.add("output");
                    // mysql
                    set.add("outfile");

                    set.add("over");
                    // oracle
                    set.add("overflow");

                    set.add("overlaps");
                    // oracle
                    set.add("own");
                    // oracle, db2
                    set.add("package");

                    set.add("pad");
                    // db2
                    set.add("padded");
                    // oracle
                    set.add("parallel");

                    set.add("parameter");
                    // db2
                    set.add("part");

                    set.add("partial");

                    set.add("partition");
                    // db2
                    set.add("partitioned");
                    // db2
                    set.add("partitioning");
                    // oracle
                    set.add("password");
                    // oracle
                    set.add("password_grace_time");
                    // oracle
                    set.add("password_life_time");
                    // oracle
                    set.add("password_lock_time");
                    // oracle
                    set.add("password_reuse_max");
                    // oracle
                    set.add("password_reuse_time");
                    // oracle
                    set.add("password_verify_function");

                    set.add("path");
                    // oracle
                    set.add("pctfree");
                    // oracle
                    set.add("pctincrease");
                    // oracle
                    set.add("pctthreshold");
                    // oracle
                    set.add("pctused");
                    // oracle
                    set.add("pctversion");
                    // oracle, sqlserver
                    set.add("percent");
                    // db2
                    set.add("period");
                    // oracle
                    set.add("permanent");
                    // db2
                    set.add("piecesize");
                    // sqlserver
                    set.add("pivot");
                    // postgresql
                    set.add("placing");
                    // oracle, sqlserver, db2
                    set.add("plan");
                    // oracle
                    set.add("plsql_debug");

                    set.add("position");
                    // oracle
                    set.add("post_transaction");

                    set.add("precesion");

                    set.add("prepare");

                    set.add("preserve");
                    // db2
                    set.add("prevval");

                    set.add("primary");
                    // sqlserver
                    set.add("print");

                    set.add("prior");
                    // db2
                    set.add("priqty");
                    // oracle
                    set.add("private");
                    // oracle
                    set.add("private_sga");
                    // oracle
                    set.add("privilege");

                    set.add("privileges");
                    // sqlserver
                    set.add("proc");

                    set.add("procedure");
                    // oracle
                    set.add("profile");
                    // db2
                    set.add("program");
                    // db2
                    set.add("psid");

                    set.add("public");
                    // mysql, oracle
                    set.add("purge");
                    // oracle
                    set.add("queue");
                    // db2
                    set.add("query");
                    // db2
                    set.add("queryno");
                    // oracle
                    set.add("quota");
                    // sqlserver
                    set.add("raiserror");

                    set.add("range");
                    // oracle
                    set.add("raw");
                    // oracle
                    set.add("rba");

                    set.add("read");

                    set.add("reads");
                    // sqlserver
                    set.add("readtext");
                    // oracle
                    set.add("readup");
                    // mysql
                    set.add("read_write");

                    set.add("real");
                    // oracle
                    set.add("rebuild");
                    // sqlserver
                    set.add("reconfigure");
                    // oracle
                    set.add("recover");
                    // oracle
                    set.add("recoverable");
                    // oracle
                    set.add("recovery");

                    set.add("recursive");

                    set.add("ref");

                    set.add("references");

                    set.add("referencing");
                    // oracle, db2
                    set.add("refresh");
                    // mysql
                    set.add("regexp");

                    set.add("relative");

                    set.add("release");
                    // db2
                    set.add("resignal");
                    // mysql, oracle, db2
                    set.add("rename");

                    set.add("repeat");
                    // mysql, oracle
                    set.add("replace");
                    // sqlserver
                    set.add("replication");
                    // mysql
                    set.add("require");
                    // oracle
                    set.add("reset");
                    // oracle
                    set.add("resetlogs");

                    set.add("resignal");
                    // oracle
                    set.add("resize");
                    // oracle
                    set.add("resource");
                    // sqlserver
                    set.add("restore");

                    set.add("restrict");
                    // oracle
                    set.add("restricted");

                    set.add("result");
                    // db2
                    set.add("result_set_locator");

                    set.add("return");
                    // oracle, postgresql
                    set.add("returning");

                    set.add("returns");
                    // oracle
                    set.add("reuse");
                    // oracle
                    set.add("reverse");
                    // sqlserver
                    set.add("revert");

                    set.add("revoke");

                    set.add("right");
                    // mysql
                    set.add("rlike");

                    set.add("role");
                    // oracle
                    set.add("roles");

                    set.add("rollback");

                    set.add("rollup");

                    set.add("routine");
                    // db2
                    set.add("round_ceiling");
                    // db2
                    set.add("round_down");
                    // db2
                    set.add("round_floor");
                    // db2
                    set.add("round_half_down");
                    // db2
                    set.add("round_half_even");
                    // db2
                    set.add("round_half_up");
                    // db2
                    set.add("round_up");
                    // oracle
                    set.add("row");
                    // sqlserver
                    set.add("rowcount");
                    // sqlserver
                    set.add("rowguidcol");
                    // oracle
                    set.add("rowid");
                    // oracle
                    set.add("rownum");

                    set.add("rows");
                    // db2
                    set.add("rowset");
                    // oracle, sqlserver
                    set.add("rule");
                    // db2
                    set.add("run");
                    // oracle
                    set.add("sample");
                    // sqlserver
                    set.add("save");

                    set.add("savepoint");
                    // oracle
                    set.add("sb4");
                    // oracle
                    set.add("scan_instances");

                    set.add("schema");
                    // mysql
                    set.add("schemas");
                    // oracle
                    set.add("scn");

                    set.add("scope");
                    // db2
                    set.add("scratchpad");

                    set.add("scroll");
                    // oracle
                    set.add("sd_all");
                    // oracle
                    set.add("sd_inhibit");
                    // oracle
                    set.add("sd_show");

                    set.add("search");

                    set.add("second");
                    // db2
                    set.add("seconds");
                    // mysql
                    set.add("second_microsecond");
                    // sqlserver
                    set.add("securityaudit");

                    set.add("section");
                    // db2
                    set.add("security");
                    // oracle
                    set.add("segment");
                    // oracle
                    set.add("seg_block");
                    // oracle
                    set.add("seg_file");

                    set.add("select");
                    // sqlserver
                    set.add("semantickeyphrasetable");
                    // sqlserver
                    set.add("semanticsimilaritydetailstable");
                    // sqlserver
                    set.add("semanticsimilaritytable");

                    set.add("sensitive");
                    // mysql
                    set.add("separator");
                    // oracle, db2
                    set.add("sequence");
                    // oracle
                    set.add("serializable");

                    set.add("session");
                    // oracle
                    set.add("session_cached_cursors");
                    // oracle
                    set.add("sessions_per_user");

                    set.add("session_user");

                    set.add("set");

                    set.add("sets");
                    // sqlserver
                    set.add("setuser");
                    // oracle
                    set.add("share");
                    // oracle
                    set.add("shared");
                    // oracle
                    set.add("shared_pool");
                    // mysql
                    set.add("show");
                    // oracle
                    set.add("shrink");
                    // sqlserver
                    set.add("shutdown");

                    set.add("signal");

                    set.add("similar");
                    // db2
                    set.add("simple");

                    set.add("size");
                    // oracle
                    set.add("skip");
                    // oracle
                    set.add("skip_unusable_indexes");

                    set.add("smallint");
                    // oracle
                    set.add("snapshot");

                    set.add("some");
                    // db2
                    set.add("source");
                    // oracle
                    set.add("sort");

                    set.add("space");
                    // mysql
                    set.add("spatial");

                    set.add("specific");
                    // oracle
                    set.add("specification");

                    set.add("specifictype");
                    // oracle
                    set.add("split");

                    set.add("sql");

                    set.add("sqlcode");

                    set.add("sqlerror");

                    set.add("sqlexception");

                    set.add("sqlstate");

                    set.add("sqlwarning");
                    // mysql
                    set.add("sql_big_result");
                    // mysql
                    set.add("sql_calc_found_rows");
                    // mysql
                    set.add("sql_small_result");
                    // oracle
                    set.add("sql_trace");
                    // mysql
                    set.add("ssl");
                    // db2
                    set.add("standard");
                    // oracle
                    set.add("standby");

                    set.add("start");
                    // mysql
                    set.add("starting");

                    set.add("state");
                    // db2
                    set.add("statement");
                    // oracle
                    set.add("statement_id");

                    set.add("static");
                    // oracle, sqlserver
                    set.add("statistics");
                    // db2
                    set.add("stay");
                    // db2
                    set.add("stogroup");
                    // oracle
                    set.add("stop");
                    // oracle
                    set.add("storage");
                    // oracle
                    set.add("store");
                    // db2
                    set.add("stores");
                    // mysql
                    set.add("straight_join");
                    // oracle
                    set.add("structure");
                    // db2
                    set.add("style");

                    set.add("submultiset");

                    set.add("substring");
                    // oracle
                    set.add("successful");

                    set.add("sum");
                    // db2
                    set.add("summary");
                    // oracle
                    set.add("switch");

                    set.add("symmetric");
                    // oracle, db2
                    set.add("synonym");
                    // oracle, db2
                    set.add("sysdate");
                    // oracle
                    set.add("sysdba");
                    // oracle
                    set.add("sysoper");

                    set.add("system");

                    set.add("system_user");
                    // db2
                    set.add("systimestamp");

                    set.add("table");
                    // oracle
                    set.add("tables");

                    set.add("tablesample");
                    // oracle, db2
                    set.add("tablespace");
                    // oracle
                    set.add("tablesapce_no");
                    // oracle
                    set.add("tabno");

                    set.add("temporary");
                    // mysql
                    set.add("terminated");
                    // sqlserver
                    set.add("textsize");
                    // oracle
                    set.add("than");
                    // oracle
                    set.add("the");

                    set.add("then");
                    // oracle
                    set.add("thread");

                    set.add("time");

                    set.add("timestamp");

                    set.add("timezone_hour");

                    set.add("timezone_minute");
                    // mysql
                    set.add("tinyblob");
                    // mysql
                    set.add("tinyint");
                    // mysql
                    set.add("tinytext");

                    set.add("to");
                    // sqlserver
                    set.add("top");
                    // oracle
                    set.add("toplevel");
                    // oracle
                    set.add("trace");
                    // oracle
                    set.add("tracing");
                    // sqlserver
                    set.add("tran");

                    set.add("trailing");

                    set.add("transaction");

                    set.add("translate");

                    set.add("translation");
                    // oracle
                    set.add("transactional");

                    set.add("treat");

                    set.add("trigger");
                    // oracle
                    set.add("triggers");

                    set.add("trim");

                    set.add("true");
                    // oracle, sqlserver, db2
                    set.add("truncate");
                    // sqlserver
                    set.add("try_convert");
                    // sqlserver
                    set.add("tsequal");
                    // oracle
                    set.add("tx");
                    // oracle, db2
                    set.add("type");
                    // oracle
                    set.add("ub2");
                    // oracle
                    set.add("uba");
                    // oracle
                    set.add("uid");
                    // oracle
                    set.add("unarchived");

                    set.add("undo");

                    set.add("union");

                    set.add("unique");

                    set.add("unknown");
                    // oracle
                    set.add("unlimited");
                    // mysql, oracle
                    set.add("unlock");
                    // sqlserver
                    set.add("unpivot");
                    // oracle
                    set.add("unrecoverable");

                    set.add("unnest");
                    // mysql
                    set.add("unsigned");

                    set.add("until");
                    // oracle
                    set.add("unusable");
                    // oracle
                    set.add("unused");
                    // oracle
                    set.add("updatable");

                    set.add("update");
                    // sqlserver
                    set.add("updatetext");

                    set.add("upper");

                    set.add("usage");
                    // mysql, oracle, sqlserver
                    set.add("use");

                    set.add("user");

                    set.add("using");
                    // mysql
                    set.add("utc_date");
                    // mysql
                    set.add("utc_time");
                    // mysql
                    set.add("utc_timestamp");
                    // oracle
                    set.add("validate");
                    // oracle
                    set.add("validation");
                    // db2
                    set.add("validproc");

                    set.add("value");

                    set.add("values");
                    // mysql
                    set.add("varbinary");
                    // db2
                    set.add("variable");
                    // postgresql
                    set.add("variadic");
                    // db2
                    set.add("variant");

                    set.add("varchar");
                    // oracle
                    set.add("varchar2");
                    // mysql
                    set.add("varcharacter");

                    set.add("varying");
                    // db2
                    set.add("vcat");

                    set.add("view");
                    // db2
                    set.add("volatile");
                    // db2
                    set.add("volumes");
                    // sqlserver
                    set.add("waitfor");

                    set.add("when");

                    set.add("whenever");

                    set.add("where");

                    set.add("while");

                    set.add("window");

                    set.add("with");

                    set.add("within");

                    set.add("without");
                    // db2
                    set.add("wlm");

                    set.add("work");

                    set.add("write");
                    // oracle
                    set.add("writedown");
                    // sqlserver
                    set.add("writetext");
                    // oracle
                    set.add("writeup");
                    // oracle
                    set.add("xid");
                    // db2
                    set.add("xmlexists");
                    // db2
                    set.add("xmlnamespaces");
                    // db2
                    set.add("xmlcast");
                    // mysql
                    set.add("xor");

                    set.add("year");
                    // db2
                    set.add("years");
                    // mysql
                    set.add("year_month");
                    // mysql
                    set.add("zerofill");

                    set.add("zone");

                    reservedWords = set;
                }
            }
        }
        return reservedWords;
    }

    public char getReservedWordEscapingBraceOpen() {
        return '\"';
    }

    public char getReservedWordEscapingBraceClose() {
        return '\"';
    }

    public String getQueryColumns() {
        return getQueryColumnNames();
    }

    public String getQueryColumn() {
        return getQueryColumnName();
    }

    public String getQueryColumnNames() {
        return "Must implement getQueryColumns() for " + getDbType();
    }

    public String getQueryColumnName() {
        return "Must implement getQueryColumn() for " + getDbType();
    }

    public String getQueryCountView() {
        return getQueryCountTable();
    }

    public String getQueryViewColumns() {
        return getQueryColumns();
    }

    public String getQueryViewColumn() {
        return getQueryColumn();
    }

}
