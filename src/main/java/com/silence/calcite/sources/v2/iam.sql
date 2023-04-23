create database if not exists `iam` default charset utf8mb4;

use `iam`;


create table if not exists `roles`
(
    `id`   bigint primary key auto_increment,
    `name` varchar(64)
) engine = innodb
  default charset utf8mb4;

create table if not exists `groups`
(
    `id`   bigint primary key auto_increment,
    `name` varchar(64)
) engine = innodb
  default charset utf8mb4;


create table if not exists `users`
(
    `id`    bigint primary key auto_increment,
    `name`  varchar(64),
    `role`  bigint,
    `group` bigint
) engine = innodb
  default charset utf8mb4;


insert into `roles`
values (1, "db-root"),
       (2, "db-admin"),
       (3, "user");
insert into `groups`
values (1, "db-g1"),
       (2, "db-g2");
insert into `users`
values (1, "db-root", 1, 1),
       (2, "db-admin", 2, 1),
       (3, "db-kk", 3, 2),
       (4, "db-silence", 3, 2);