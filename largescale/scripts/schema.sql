-- DDL for large-scale database
-- Run: mysql -u root < largescale/scripts/schema.sql

CREATE DATABASE IF NOT EXISTS `large-scale` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE `large-scale`;

-- ============================================================
-- User table
-- ============================================================
CREATE TABLE IF NOT EXISTS `user` (
    `user_id`     INT          NOT NULL,
    `username`    VARCHAR(30)  NOT NULL,
    `email`       VARCHAR(30)  NOT NULL,
    `nickname`    VARCHAR(10)  NOT NULL,
    `group_id`    INT          NOT NULL,
    `user_status` CHAR(1)      NOT NULL,
    `create_date` DATETIME(6)  NOT NULL,
    `update_date` DATETIME(6)  NOT NULL,
    PRIMARY KEY (`user_id`),
    INDEX `user_idx04` (`username`),
    INDEX `user_idx05` (`create_date` DESC, `user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================
-- Account table
-- ============================================================
CREATE TABLE IF NOT EXISTS `account` (
    `account_id`               INT           NOT NULL AUTO_INCREMENT,
    `account_number`           VARCHAR(15)   NOT NULL,
    `user_id`                  INT           NOT NULL,
    `account_type`             CHAR(1)       NOT NULL,
    `memo`                     VARCHAR(200)  NULL,
    `balance`                  BIGINT        NOT NULL DEFAULT 0,
    `create_date`              DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    `recent_transaction_date`  DATETIME(6)   NULL,
    PRIMARY KEY (`account_id`),
    INDEX `account_idx01` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================
-- Transaction table
-- ============================================================
CREATE TABLE IF NOT EXISTS `transaction` (
    `transaction_id`       INT           NOT NULL AUTO_INCREMENT,
    `sender_account`       VARCHAR(20)   NOT NULL,
    `receiver_account`     VARCHAR(20)   NOT NULL,
    `sender_swift_code`    VARCHAR(11)   NOT NULL,
    `receiver_swift_code`  VARCHAR(11)   NOT NULL,
    `sender_name`          VARCHAR(20)   NOT NULL,
    `receiver_name`        VARCHAR(20)   NOT NULL,
    `amount`               BIGINT        NOT NULL,
    `memo`                 VARCHAR(200)  NULL,
    `transaction_date`     DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (`transaction_id`),
    INDEX `tx_idx01` (`transaction_date` DESC, `transaction_id`),
    INDEX `tx_idx02` (`sender_account`, `transaction_date` DESC, `transaction_id`),
    INDEX `tx_idx03` (`receiver_account`, `transaction_date` DESC, `transaction_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================
-- Migration test tables (same structure via LIKE)
-- ============================================================
CREATE TABLE IF NOT EXISTS `account_migration_test_0` LIKE `account`;
CREATE TABLE IF NOT EXISTS `account_migration_test_1` LIKE `account`;

CREATE TABLE IF NOT EXISTS `transaction_migration_test_0` LIKE `transaction`;
CREATE TABLE IF NOT EXISTS `transaction_migration_test_1` LIKE `transaction`;

-- ============================================================
-- Sharded transaction tables
-- ============================================================
CREATE TABLE IF NOT EXISTS `transaction_sd_0` LIKE `transaction`;
CREATE TABLE IF NOT EXISTS `transaction_sd_1` LIKE `transaction`;
