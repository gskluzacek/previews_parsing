CREATE DATABASE if not exists `previews` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs */;

use previews;

CREATE TABLE `hdg_hrch_import_template` (
  `row_num` int(11) NOT NULL,
  `file_name` varchar(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,
  `pvh_id` int(11) NOT NULL,
  `pvl_seq` int(11) NOT NULL,
  `pvl_id` int(11) NOT NULL,
  `parent_pvl_id` int(11) DEFAULT NULL,
  `pg_nbr` int(11) NOT NULL,
  `hdg_lvl` int(11) DEFAULT NULL,
  `detail_items_ind` tinyint(1) DEFAULT NULL,
  `dup_pvl_id` int(11) DEFAULT NULL,
  `pvhh_id` int(11) DEFAULT NULL,
  `line_text` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,
  `indent` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs DEFAULT NULL,
  PRIMARY KEY (`pvh_id`,`pvl_seq`),
  UNIQUE KEY `hdg_hrch_import_1_1_pvl_id_uindex` (`pvl_id`),
  UNIQUE KEY `hdg_hrch_import_1_1_row_num_uindex` (`row_num`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE `previews_basic_dtl` (
  `pvb_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `pvl_id` bigint(20) NOT NULL,
  `promo_cd` varchar(15) COLLATE utf8mb4_0900_as_cs DEFAULT NULL,
  `sol_code` varchar(10) COLLATE utf8mb4_0900_as_cs NOT NULL COMMENT 'there should be no spaces in the value',
  `sol_text` varchar(300) COLLATE utf8mb4_0900_as_cs NOT NULL,
  `release_dt` date DEFAULT NULL,
  `unit_price_raw` varchar(50) COLLATE utf8mb4_0900_as_cs DEFAULT NULL,
  PRIMARY KEY (`pvb_id`),
  UNIQUE KEY `previews_basic_dtl_pvl_id_uindex` (`pvl_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE `previews_hdg_hrch` (
  `pvhh_tid` int(11) NOT NULL AUTO_INCREMENT,
  `pvl_id` int(11) NOT NULL,
  `pvhh_id` int(11) NOT NULL,
  `parent_pvhh_id` int(11) DEFAULT NULL,
  `hrch_level` int(11) NOT NULL,
  `heading_nm` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,
  `detail_items_ind` tinyint(1) NOT NULL,
  `valid_from` date NOT NULL,
  `valid_to` date NOT NULL,
  PRIMARY KEY (`pvhh_tid`),
  UNIQUE KEY `previews_hdg_hrch_pvhh_id_valid_from_uindex` (`pvhh_id`,`valid_from`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE `previews_hdr` (
  `pvh_id` int(11) NOT NULL AUTO_INCREMENT,
  `ident_typ` smallint(6) NOT NULL,
  `ident_line` smallint(6) NOT NULL,
  `txt_ident` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs DEFAULT NULL,
  `txt_mo` varchar(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs DEFAULT NULL,
  `txt_yr` int(11) DEFAULT NULL,
  `txt_volume` int(11) DEFAULT NULL,
  `txt_vol_issue` int(11) DEFAULT NULL,
  `txt_issue` int(11) DEFAULT NULL,
  `txt_period` date DEFAULT NULL,
  `txt_name` varchar(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs DEFAULT NULL,
  `fn_ident` varchar(50) COLLATE utf8mb4_0900_as_cs NOT NULL,
  `fn_mo` varchar(3) COLLATE utf8mb4_0900_as_cs NOT NULL,
  `fn_yr` int(11) NOT NULL,
  `fn_volume` int(11) NOT NULL,
  `fn_vol_issue` int(11) NOT NULL,
  `fn_issue` int(11) NOT NULL,
  `fn_period` date NOT NULL,
  `fn_name` varchar(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,
  `proc_sts` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,
  `fn_path` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,
  PRIMARY KEY (`pvh_id`),
  UNIQUE KEY `previews_hdr_file_name_uindex` (`fn_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE `previews_lines` (
  `pvl_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `pvh_id` int(11) NOT NULL,
  `pvl_seq` int(11) NOT NULL,
  `pv_type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs DEFAULT NULL,
  `pg_nbr` int(11) DEFAULT NULL,
  `pvhh_id` int(11) DEFAULT NULL,
  `line_text` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,
  PRIMARY KEY (`pvl_id`),
  UNIQUE KEY `previews_lines_pvh_id_pvl_seq_uindex` (`pvh_id`,`pvl_seq`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE `pvhh_seq` (
  `id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

insert into pvhh_seq values(0);