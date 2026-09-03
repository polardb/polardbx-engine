# Helper for feature_lob_prealloc_calc_pages.test
# Inputs:  $blob_len, $expected_pages
# Effect:  Snapshot Lizard_lob_prealloc_pages, INSERT a blob of $blob_len bytes,
#          snapshot again, assert the delta equals $expected_pages.

SET @before = (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Lizard_lob_prealloc_pages');

--eval INSERT INTO t1 (data) VALUES (REPEAT('x', $blob_len))

SET @after = (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Lizard_lob_prealloc_pages');

--let $delta_expected = $expected_pages
if (`SELECT CONVERT(@after, SIGNED) - CONVERT(@before, SIGNED) != $delta_expected`)
{
  --echo Unexpected prealloc delta for blob_len=$blob_len, expected=$expected_pages
  eval SELECT CONVERT(@after, SIGNED) - CONVERT(@before, SIGNED) AS actual_delta;
  --die
}
