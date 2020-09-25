package com.dbist.demo.entity;

import java.util.Date;

import org.dbist.annotation.Column;
import org.dbist.annotation.ColumnType;
import org.dbist.annotation.Index;
import org.dbist.annotation.PrimaryKey;
import org.dbist.annotation.Relation;
import org.dbist.annotation.Table;

import com.dbist.demo.entity.ref.UserRef;

import lombok.Data;
import lombok.experimental.Accessors;

import static org.dbist.annotation.ColumnType.DATETIME;
import static org.dbist.annotation.GenerationRule.UUID;

@Data
@Accessors(chain = true)
@Table(name = "tb_notice", idStrategy = UUID, indexes = {
    @Index(name = "ix_notice_0", columnList = "title")
})
public class Notice {

    @PrimaryKey
    @Column(length = 50, nullable = false)
    private String id;

    @Column(length = 400, nullable = false)
    private String title;

    @Column(length = 500)
    private String subtitle;

    @Column(type = ColumnType.TEXT)
    private String contents;

    @Column(length = 20)
    private String category;

    @Column(length = 50)
    private String creatorId;

    @Column(length = 50)
    private String updaterId;

    @Column(type = DATETIME)
    private Date createdAt;

    @Column(type = DATETIME)
    private Date updatedAt;

    @Relation(field = "creatorId")
    private UserRef creator;

    @Relation(field = "updaterId")
    private UserRef updater;
}
