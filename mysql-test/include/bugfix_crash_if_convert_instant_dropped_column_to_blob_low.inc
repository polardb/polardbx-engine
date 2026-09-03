DROP TABLE IF EXISTS `t2`;
CREATE TABLE `t2` LIKE `t1`;

# Insert 8 records
# 1 & 2. `c1` is longest but still internal.
# 3 & 4. `c1` is longest and converted to external.
# 5 & 6. `c1` is short.
# 7 & 8. `c1` is NULL.
INSERT INTO `t2` (id, `c1`, `c2`) VALUES
  (1, repeat('x', 8000), NULL),
  (2, repeat('x', 8000), NULL),
  (3, repeat('x', 8000), repeat('x', 7000)),
  (4, repeat('x', 8000), repeat('x', 7000)),
  (5, 'x', NULL),
  (6, 'x', NULL),
  (7, NULL, NULL),
  (8, NULL, NULL);

# Instant drop column `c1`
ALTER TABLE `t2` DROP COLUMN `c1`, ALGORITHM=INSTANT;

# Update 6 records
# 1. The new record's whole length is more than 8126 to call
#    page_zip_rec_needs_ext. Before this bugfix, dropped `c1` must be added
#    into big_rec and cause crash as any other column's length is less than
#    `c1`.
# 2. The new record will be internal without blob.
# 3 & 4. Whether the new record is external or internal, the blob of
#    dropped `c1` in old record will be residual.
# 5 & 6 & 7 & 8. Validate no crash whether dropped column is NULL or not.
UPDATE `t2` SET `c2`=repeat('y', 7000), `c3`=repeat('y', 6000) where id = 1 or id = 3 or id = 5 or id = 7;
UPDATE `t2` SET `c2`=repeat('y', 7000) where id = 2 or id = 4 or id = 6 or id = 8;

--let $CHECKSUM=`CHECKSUM TABLE t2`
