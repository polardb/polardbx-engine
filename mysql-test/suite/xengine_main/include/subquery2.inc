#
# Bug #24484: Aggregate function used in column list subquery gives erroneous
# error
#
CREATE TABLE t1 (a int, b INT, c CHAR(10) NOT NULL, PRIMARY KEY (a, b));
INSERT INTO t1 VALUES (1,1,'a'), (1,2,'b'), (1,3,'c'), (1,4,'d'), (1,5,'e'),
  (2,1,'f'), (2,2,'g'), (2,3,'h'), (3,4,'i'),(3,3,'j'), (3,2,'k'), (3,1,'l'),
  (1,9,'m');
CREATE TABLE t2 (a int, b INT, c CHAR(10) NOT NULL, PRIMARY KEY (a, b));
INSERT INTO t2 SELECT * FROM t1;

# Gives error, but should work since it is (a, b) is the PK so only one
# given match possible
SELECT a, MAX(b), (SELECT t.c FROM t1 AS t WHERE t1.a=t.a AND t.b=MAX(t1.b))
  as test FROM t1 GROUP BY a;
--source include/turn_off_only_full_group_by.inc
SELECT * FROM t1 GROUP by t1.a
  HAVING (MAX(t1.b) > (SELECT MAX(t2.b) FROM t2 WHERE t2.c < t1.c
                                                HAVING MAX(t2.b+t1.a) < 10));

