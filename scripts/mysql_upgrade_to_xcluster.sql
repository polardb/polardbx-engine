set force_revise = ON;
set sql_log_bin = OFF;
CREATE TABLE IF NOT EXISTS mysql.consensus_info (
  number_of_lines INTEGER UNSIGNED NOT NULL COMMENT 'Number of lines in the file or rows in the table. Used to version table definitions.', 
  vote_for BIGINT UNSIGNED COMMENT 'current vote for', current_term BIGINT UNSIGNED COMMENT 'current term',
    recover_status BIGINT UNSIGNED COMMENT 'recover status', last_leader_term BIGINT UNSIGNED COMMENT 'last leader term', 
    start_apply_index BIGINT UNSIGNED COMMENT 'start apply index', cluster_id BIGINT UNSIGNED COMMENT 'cluster identifier',
    cluster_info varchar(6000) COMMENT 'cluster config information', cluster_learner_info varchar(6000) COMMENT 'cluster learner config information',
    cluster_config_recover_index BIGINT UNSIGNED COMMENT 'cluster config recover index',
    PRIMARY KEY(number_of_lines)
)engine=INNODB CHARACTER SET utf8mb4 comment="cluster consensus information table";
    
ALTER TABLE mysql.slave_relay_log_info ADD Consensus_apply_index BIGINT UNSIGNED NOT  NULL COMMENT 'The consensus log applyed index in the consensus log';
ALTER TABLE mysql.slave_worker_info ADD Checkpoint_consensus_apply_index BIGINT UNSIGNED NOT  NULL COMMENT 'Checkpoint index';
ALTER TABLE mysql.slave_worker_info ADD Consensus_apply_index BIGINT UNSIGNED NOT  NULL COMMENT 'The consensus log applyed index in the consensus log';
truncate table mysql.slave_master_info;
truncate table mysql.slave_relay_log_info;
truncate table mysql.slave_worker_info;
flush tables;
flush logs;
set global innodb_fast_shutdown = 0;
