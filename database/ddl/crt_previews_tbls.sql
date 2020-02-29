CREATE DATABASE `previews` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs */ /*!80016 DEFAULT ENCRYPTION='N' */;

use previews;

CREATE TABLE `hdg_hrch_import_template` (
  `row_num` int(11) NOT NULL,
  `file_name` varchar(25) COLLATE utf8mb4_0900_as_cs NOT NULL,
  `pvh_id` int(11) NOT NULL,
  `pvl_id` int(11) NOT NULL,
  `pvl_seq` int(11) NOT NULL,
  `pg_nbr` int(11) NOT NULL,
  `hdg_lvl` int(11) DEFAULT NULL,
  `dup_pvl_id` int(11) DEFAULT NULL,
  `line_text` varchar(500) COLLATE utf8mb4_0900_as_cs NOT NULL,
  `indent` varchar(500) COLLATE utf8mb4_0900_as_cs DEFAULT NULL,
  PRIMARY KEY (`pvh_id`,`row_num`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE `previews_hdg_hrch` (
  `pvhh_tid` int(11) NOT NULL AUTO_INCREMENT,
  `pvhh_id` int(11) NOT NULL,
  `hrch_level` int(11) NOT NULL,
  `heading_nm` varchar(100) COLLATE utf8mb4_0900_as_cs NOT NULL,
  `parent_pvhh_id` int(11) DEFAULT NULL,
  `valid_from` date NOT NULL,
  `valid_to` date NOT NULL,
  PRIMARY KEY (`pvhh_tid`),
  UNIQUE KEY `previews_hdg_hrch_pvhh_id_valid_from_uindex` (`pvhh_id`,`valid_from`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE `previews_hdr` (
  `pvh_id` int(11) NOT NULL AUTO_INCREMENT,
  `ident_typ` smallint(6) NOT NULL,
  `ident_line` smallint(6) DEFAULT NULL,
  `raw_ident` varchar(100) COLLATE utf8mb4_0900_as_cs DEFAULT NULL,
  `ident_str` varchar(50) COLLATE utf8mb4_0900_as_cs DEFAULT NULL,
  `ident_mo` varchar(3) COLLATE utf8mb4_0900_as_cs DEFAULT NULL,
  `ident_yr` int(11) DEFAULT NULL,
  `running_issue` int(11) DEFAULT NULL,
  `volume` int(11) DEFAULT NULL,
  `issue` int(11) DEFAULT NULL,
  `fn_period` date NOT NULL,
  `file_name` varchar(25) COLLATE utf8mb4_0900_as_cs NOT NULL,
  `proc_sts` varchar(20) COLLATE utf8mb4_0900_as_cs NOT NULL,
  `file_path` varchar(500) COLLATE utf8mb4_0900_as_cs NOT NULL,
  PRIMARY KEY (`pvh_id`),
  UNIQUE KEY `previews_hdr_file_name_uindex` (`file_name`)
) ENGINE=InnoDB AUTO_INCREMENT=136 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE `previews_lines` (
  `pvl_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `pvh_id` int(11) NOT NULL,
  `pvl_seq` int(11) NOT NULL,
  `pv_type` varchar(50) COLLATE utf8mb4_0900_as_cs DEFAULT NULL,
  `pg_nbr` int(11) DEFAULT NULL,
  `line_text` varchar(1000) COLLATE utf8mb4_0900_as_cs NOT NULL,
  PRIMARY KEY (`pvl_id`),
  UNIQUE KEY `previews_lines_pvh_id_pvl_seq_uindex` (`pvh_id`,`pvl_seq`)
) ENGINE=InnoDB AUTO_INCREMENT=436822 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE `pvhh_seq` (
  `id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

