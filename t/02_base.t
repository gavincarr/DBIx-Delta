
my $test_count;
BEGIN { $test_count = 14 }

use File::Basename;
use File::Spec;
use Test::More tests => $test_count;
use YAML qw(LoadFile Dump);
use Test::Deep;
use DBI;

SKIP: {
  skip "no DBD::SQLite", $test_count unless eval { require DBD::SQLite };

  my $testdir = File::Spec->rel2abs( File::Spec->catdir( dirname($0), 't' . basename($0) ) );
  $testdir =~ s/\.t$//;
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
  my ($count, $delta, $update);

  # Check deltas to apply
  ($count, $delta) = TestDelta->run('-q');
  is($count, 2, "found 2 deltas to apply");

  # Apply first
  ($count, $update) = TestDelta->run('-qu', $delta->[0]);
  is($count, 1, "1 delta applied ok");
  is($update->[0], $delta->[0], "delta applied is '$update->[0]'");

  # Check database
  # Why does this fail unless I reconnect???
  ok($dbh = DBI->connect("dbi:SQLite:$db", '', ''), 'dbh reconnect ok');
  my $data = $dbh->selectall_arrayref(q(select * from aa order by id)); 
  my $expected = LoadFile('../expected/aa.yml');
  cmp_deeply($data, $expected, 'aa table data ok');

  # Check deltas to apply
  $count = TestDelta->run('-q');
  is($count, 1, "found 1 delta to apply");

  # Apply rest
  ($count, $update) = TestDelta->run('-qu');
  is($count, 1, "1 delta applied ok");
  is($update->[0], $delta->[1], "delta applied is '$update->[0]'");

  # Check database
  # Why does this fail unless I reconnect???
  ok($dbh = DBI->connect("dbi:SQLite:$db", '', ''), 'dbh reconnect ok');
  $data = $dbh->selectall_arrayref(q(select * from bb order by id)); 
  $expected = LoadFile('../expected/bb.yml');
  cmp_deeply($data, $expected, 'bb table data ok');

  # Cleanup
  $dbh->disconnect;
  unlink $db unless $ENV{TEST_DELTA_KEEP_DB};
}

