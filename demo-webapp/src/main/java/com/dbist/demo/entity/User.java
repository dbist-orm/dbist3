package com.dbist.demo.entity;

import java.util.Date;

import org.dbist.annotation.Column;
import org.dbist.annotation.Index;
import org.dbist.annotation.PrimaryKey;
import org.dbist.annotation.Table;

import lombok.Data;
import lombok.experimental.Accessors;

import static org.dbist.annotation.ColumnType.DATETIME;
import static org.dbist.annotation.GenerationRule.UUID;

@Data
@Accessors(chain = true)
@Table(name = "tb_user", idStrategy = UUID, indexes = {
    @Index(name = "ix_user_0", columnList = "name")
})
public class User {

    @PrimaryKey
    @Column(length = 50, nullable = false)
    private String id;

    @Column(nullable = false)
    private String password;

    @Column(length = 50)
    private String name;

    @Column(length = 50)
    private String status;

    @Column(length = 50)
    private String creatorId;

    @Column(length = 50)
    private String updaterId;

    @Column(type = DATETIME)
    private Date createdAt = new Date();

    @Column(type = DATETIME)
    private Date updatedAt = new Date();
}
