DROP TABLE if exists `lock_table_0`;
CREATE TABLE `lock_table_0` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `lock_key` varchar(255) NOT NULL,
  `xid` bigint(20) DEFAULT NULL,
  `node` varchar(255) DEFAULT NULL,
  `table` varchar(128) NOT NULL,
  `context` varchar(2000) DEFAULT '',
  `client_id` varchar(64) DEFAULT '',
  `create_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `update_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_lock` (`node`,`table`,`lock_key`),
  KEY `in_xid` (`xid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE if exists `lock_table_1`;
CREATE TABLE `lock_table_1` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `lock_key` varchar(255) NOT NULL,
  `xid` bigint(20) DEFAULT NULL,
  `node` varchar(255) DEFAULT NULL,
  `table` varchar(128) NOT NULL,
  `context` varchar(2000) DEFAULT '',
  `client_id` varchar(64) DEFAULT '',
  `create_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `update_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_lock` (`node`,`table`,`lock_key`),
  KEY `in_xid` (`xid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE if exists `transaction_info_0`;
CREATE TABLE `transaction_info_0` (
  `xid` bigint(20) NOT NULL AUTO_INCREMENT,
  `status` tinyint(4) DEFAULT NULL,
  `client_id` varchar(64) DEFAULT '',
  `create_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `update_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`xid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE if exists `transaction_info_1`;
CREATE TABLE `transaction_info_1` (
  `xid` bigint(20) NOT NULL AUTO_INCREMENT,
  `status` tinyint(4) DEFAULT NULL,
  `client_id` varchar(64) DEFAULT '',
  `create_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `update_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`xid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE if exists `reundo_log`;
CREATE TABLE `reundo_log` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `lock_key` varchar(255) NOT NULL,
  `xid` varchar(100) NOT NULL,
  `context` varchar(2000) DEFAULT NULL,
  `reundo_log` longblob NOT NULL,
  `table` varchar(128) NOT NULL,
  `status` tinyint(4) DEFAULT NULL,
  `client_id` varchar(64) DEFAULT NULL,
  `create_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `update_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  KEY `un_xid` (`xid`),
  KEY `un_undo_log` (`table`,`lock_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE if exists `test_raw`;
CREATE TABLE `test_raw` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `test` varchar(255) NOT NULL,
  `xid` varchar(100) NOT NULL,
  `pidal_c` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY xid(xid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE if exists `test_s_0`;
CREATE TABLE `test_s_0` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(255) NOT NULL,
  `name` varchar(100) NOT NULL,
  `pidal_c` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE if exists `test_s_1`;
CREATE TABLE `test_s_1` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(255) NOT NULL,
  `name` varchar(100) NOT NULL,
  `pidal_c` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE if exists `test_s_2`;
CREATE TABLE `test_s_2` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(255) NOT NULL,
  `name` varchar(100) NOT NULL,
  `pidal_c` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE if exists `test_s_3`;
CREATE TABLE `test_s_3` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(255) NOT NULL,
  `name` varchar(100) NOT NULL,
  `pidal_c` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE if exists `test_d_0`;
CREATE TABLE `test_d_0` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) NOT NULL,
  `topic_id` bigint(20) NOT NULL,
  `status` int(4) NOT NULL DEFAULT '0',
  `pidal_c` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `tu` (`user_id`,`topic_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE if exists `test_d_1`;
CREATE TABLE `test_d_1` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) NOT NULL,
  `topic_id` bigint(20) NOT NULL,
  `status` int(4) NOT NULL DEFAULT '0',
  `pidal_c` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `tu` (`user_id`,`topic_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE if exists `test_d_2`;
CREATE TABLE `test_d_2` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) NOT NULL,
  `topic_id` bigint(20) NOT NULL,
  `status` int(4) NOT NULL DEFAULT '0',
  `pidal_c` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `tu` (`user_id`,`topic_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE if exists `test_d_3`;
CREATE TABLE `test_d_3` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) NOT NULL,
  `topic_id` bigint(20) NOT NULL,
  `status` int(4) NOT NULL DEFAULT '0',
  `pidal_c` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `tu` (`user_id`,`topic_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE if exists `test_d1_0`;
CREATE TABLE `test_d1_0` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) NOT NULL,
  `topic_id` bigint(20) NOT NULL,
  `status` int(4) NOT NULL DEFAULT '0',
  `pidal_c` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `tu` (`user_id`,`topic_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE if exists `test_d1_1`;
CREATE TABLE `test_d1_1` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) NOT NULL,
  `topic_id` bigint(20) NOT NULL,
  `status` int(4) NOT NULL DEFAULT '0',
  `pidal_c` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `tu` (`user_id`,`topic_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE if exists `test_d1_2`;
CREATE TABLE `test_d1_2` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) NOT NULL,
  `topic_id` bigint(20) NOT NULL,
  `status` int(4) NOT NULL DEFAULT '0',
  `pidal_c` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `tu` (`user_id`,`topic_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE if exists `test_d1_3`;
CREATE TABLE `test_d1_3` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) NOT NULL,
  `topic_id` bigint(20) NOT NULL,
  `status` int(4) NOT NULL DEFAULT '0',
  `pidal_c` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `tu` (`user_id`,`topic_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
