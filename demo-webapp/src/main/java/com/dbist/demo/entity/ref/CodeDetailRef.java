package com.dbist.demo.entity.ref;

import org.dbist.annotation.PrimaryKey;
import org.dbist.annotation.Table;

import lombok.Data;

@Data
@Table(name = "tb_code_detail", isRef = true)
public class CodeDetailRef {
    @PrimaryKey
    private String id;
    private String groupId;
    private String name;
    private String value;
}
