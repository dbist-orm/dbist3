package com.dbist.demo.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.dbist.ddl.Ddl;
import org.dbist.dml.Dml;
import org.dbist.dml.Page;
import org.dbist.dml.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.dbist.demo.entity.User;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Transactional
@RestController
@RequestMapping("/api/user")
//@ResponseStatus(HttpStatus.OK)
public class UserController {

    @Autowired
    private Dml dml;

    @Autowired
    private Ddl ddl;

    @GetMapping(value = "/index")
    public Page<User> index() {
        return null;
    }

    @GetMapping(value = "/{id:.+}")
    public User findOne(@PathVariable String id) throws Exception {
        {
            User user = dml.select(User.class, id);
            Optional.ofNullable(user).ifPresent(v -> log.debug(v.toString()));
        }
        {
            User param = new User().setId(id);
            User user = dml.select(param);
            Optional.ofNullable(user).ifPresent(v -> log.debug(v.toString()));
        }
        {
            User user = dml.select("TB_USER", id, User.class);
            Optional.ofNullable(user).ifPresent(v -> log.debug(v.toString()));
        }
        {
            Query query = new Query();
            dml.select(query);
        }


        return null;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public User create(@RequestBody User user) {
        ddl.createTable(User.class);
        return user;
    }

    @PostMapping(value = "/multiple")
    public List<User> createMultiple(@RequestBody List<User> list) throws Exception {
        list = new ArrayList<>();

        for (int i = 1; i <= 100; i++) {
            User user = new User()
                .setId("id-" + i)
                .setName("name-" + i)
                .setPassword("pass-" + i)
                .setStatus("status-" + i);

            list.add(user);
        }

        dml.insertBatch(list);

        return list;
    }

    @PatchMapping
    public User update(@RequestBody User user) {
        return null;
    }

    @PatchMapping(value = "/multiple")
    public Boolean updateMultiple(@RequestBody List<User> list) {
        return null;
    }

    @DeleteMapping
    public void delete(@PathVariable String id) {
    }

    @DeleteMapping(value = "/multiple")
    public void deleteMultiple(@PathVariable List<String> list) {
    }
}