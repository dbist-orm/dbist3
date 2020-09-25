package com.dbist.demo.entity;

import java.util.Date;

import org.dbist.annotation.Column;
import org.dbist.annotation.ColumnType;
import org.dbist.annotation.GenerationRule;
import org.dbist.annotation.Index;
import org.dbist.annotation.PrimaryKey;
import org.dbist.annotation.Table;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
@Table(name = "tb_code_detail", idStrategy = GenerationRule.UUID, indexes = {
    @Index(name = "ix_code_detail_0", columnList = "group_id"),
    @Index(name = "ix_code_detail_1", columnList = "group_id, name", unique = true)
})
public class CodeDetail {

    @PrimaryKey
    @Column(length = 50, nullable = false)
    private String id;

    @PrimaryKey
    @Column(length = 50, nullable = false)
    private String parentId;

    @Column(length = 50, nullable = false)
    private String name;

    @Column(length = 50, nullable = false)
    private String value;

    @Column(length = 50)
    private String creatorId;

    @Column(length = 50)
    private String updaterId;

    @Column(type = ColumnType.DATETIME)
    private Date createdAt;

    @Column(type = ColumnType.DATETIME)
    private Date updatedAt;
}
