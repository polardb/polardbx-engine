--echo Check the size of the vardir
--echo The output size is in unit blocks
perl;
#!/usr/bin/perl
use warnings;
use strict;
use File::Find;
my $dir = $ENV{'MYSQLTEST_VARDIR'};
my $size = 0;
find( sub { $size += -f $_ ? -s _ : 0 }, $dir );

# allow 2 existing sst files at most
if ( $size < 3000000000 )
        { print "TRUE", "\n";}
else   
        { print "FALSE $size", "\n";}

EOF
