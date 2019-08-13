package com.taoguba;

import net.dubboclub.catmonitor.DubboCat;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

/**
 *
 * EnableEncryptableProperties  数据库加密解析注解
 * EnableTransactionManagement  启动事务注解(回滚)
 */
@SpringBootApplication
public class CatApplication {

    public static void main(String[] args) throws IOException {
        SpringApplication.run(CatApplication.class, args);
        DubboCat.enable();
    }
}
