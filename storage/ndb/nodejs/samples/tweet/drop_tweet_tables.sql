-- Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

-- tables must be dropped in reverse order of dependencies

use test;

DROP TABLE if exists follow;
DROP TABLE if exists hashtag;
DROP TABLE if exists mention;
DROP TABLE if exists tweet;
DROP TABLE if exists author;

