let $have_consensus= `SELECT @@enable_consensus`;
if (!$have_consensus)
{
  --skip Test requires 'have_consensus'
}
