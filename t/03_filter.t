
my $test_count;
BEGIN { $test_count = 9 }

use strict;
use File::Basename;
use File::Spec;
use Test::More tests => $test_count;
use YAML qw(LoadFile Dump);
use Test::Deep;
use DBI;

SKIP: {
  skip "no DBD::SQLite", $test_count unless eval { require DBD::SQLite };

  my $testdir = File::Spec->rel2abs( File::Spec->catdir( dirname($0), 't' . basename($0) ) );
  $testdir =~ s/\..*?$//;
  die "cannot find testdir '$testdir'" unless -d $testdir;

  my $db = "$testdir/delta.db";
  $ENV{TEST_DELTA_DB} = "$testdir/delta.db";
  unshift @INC, "$testdir/lib";
  require_ok( 'TestDelta' );

  # Setup
  unlink $db if -f $db;
  my $dbh = DBI->connect("dbi:SQLite:$db", '', '');
  ok($dbh, "connect to $db ok");
  ok($dbh->do("create table delta ( delta_id text primary key, delta_tables text not null, delta_apply_ts timestamp default CURRENT_TIMESTAMP )"), 'create delta table ok');

  ok(chdir("$testdir/delta"), "chdir to $testdir/delta ok");
  my ($count, $delta, $statements, $expected);

  # Check deltas to apply
  ($count, $delta) = TestDelta->run('-q');
  is($count, 2, "found 2 deltas to apply");

  # Apply first
  ($count, $statements) = TestDelta->run('-qs', $delta->[0]);
  is($count, 3, "3 statements applied");
  $expected = LoadFile('../expected/aa.yml');
  cmp_deeply($statements, $expected, 'aa statements');

  # Apply second
  ($count, $statements) = TestDelta->run('-qs', $delta->[1]);
  is($count, 6, "6 statements applied");
  $expected = LoadFile('../expected/bb.yml');
  cmp_deeply($statements, $expected, 'bb statements');
# print Dump $statements;

  # Cleanup
  $dbh->disconnect;
  unlink $db unless $ENV{TEST_DELTA_KEEP_DB};
}

