CREATE DATABASE `previews` DEFAULT CHARACTER SET utf8mb4 collate utf8mb4_0900_as_cs;

CREATE TABLE `previews_hdg_hrch` (
  `pvhh_tid` int(11) NOT NULL AUTO_INCREMENT,
  `pvhh_id` int(11) NOT NULL,
  `hrch_level` int(11) NOT NULL,
  `heading_nm` varchar(100) NOT NULL,
  `parent_pvhh_id` int(11) DEFAULT NULL,
  `valid_from` date NOT NULL,
  `valid_to` date NOT NULL,
  PRIMARY KEY (`pvhh_tid`),
  UNIQUE KEY `previews_hdg_hrch_pvhh_id_valid_from_uindex` (`pvhh_id`,`valid_from`)
) ENGINE=InnoDB;

CREATE TABLE `previews_hdr` (
  `pvh_id` int(11) NOT NULL AUTO_INCREMENT,
  `ident_typ` smallint(6) NOT NULL,
  `ident_line` smallint(6) DEFAULT NULL,
  `raw_ident` varchar(100) DEFAULT NULL,
  `ident_str` varchar(50) DEFAULT NULL,
  `ident_mo` varchar(3) DEFAULT NULL,
  `ident_yr` int(11) DEFAULT NULL,
  `running_issue` int(11) DEFAULT NULL,
  `volume` int(11) DEFAULT NULL,
  `issue` int(11) DEFAULT NULL,
  `fn_period` date NOT NULL,
  `file_name` varchar(25) NOT NULL,
  `proc_sts` varchar(20) NOT NULL,
  `file_path` varchar(500) NOT NULL,
  PRIMARY KEY (`pvh_id`),
  UNIQUE KEY `previews_hdr_file_name_uindex` (`file_name`)
) ENGINE=InnoDB;

CREATE TABLE `previews_lines` (
  `pvl_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `pvh_id` int(11) NOT NULL,
  `pvl_seq` int(11) NOT NULL,
  `line_text` varchar(1000) NOT NULL,
  PRIMARY KEY (`pvl_id`),
  UNIQUE KEY `previews_lines_pvh_id_pvl_seq_uindex` (`pvh_id`,`pvl_seq`)
) ENGINE=InnoDB;

CREATE TABLE `pvhh_seq` (
  `id` int(11) NOT NULL
) ENGINE=InnoDB;
