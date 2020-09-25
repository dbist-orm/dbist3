package com.dbist.demo.entity.ref;

import org.dbist.annotation.PrimaryKey;
import org.dbist.annotation.Table;

import lombok.Data;

@Data
@Table(name = "tb_user", isRef = true)
public class UserRef {
    @PrimaryKey
    private String id;
    private String name;
}
