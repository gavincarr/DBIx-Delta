#
# Package to apply outstanding database deltas to a database instance.
#

package DBIx::Delta;

use Getopt::Std;
use File::Basename;
use IO::File;
use DBI;
use strict;

use vars qw($VERSION);
$VERSION = '0.5';

# abstract connect() - should be overridden with a sub returning a valid $dbh
sub connect
{
    die "connect() is an abstract method that must be provided by a subclass";
}

sub _die
{
    my $self = shift;
    $self->_disconnect;
    die join ' ', @_;
}

sub _disconnect 
{
    my $self = shift;
    $self->{dbh}->disconnect;           # unless $self->{test_mode};
}

# For subclassing to localise statements e.g. mysql grants in dev might need to
#   use different IP addresses in production
#   e.g. s/^\s* (grant\b.*?) localhost /${1}192.168.0.1/ 
sub filter_statement
{
    my $self = shift;
    shift;
}

# Parse arguments
sub parse_args
{
    my $self = shift;
    @ARGV = @_ if @_;

    my %opts = ();
    getopts('?bdfhnqtu',\%opts);

    if ($opts{'?'} || $opts{h}) {
      print "usage: " . basename($0) . " [-qbd] [-n] [-f] [-u] [<delta> ...]\n";
      exit 1;
    }

    $self->{brief}      = $opts{b} || '';
    $self->{debug}      = $opts{d} || '';
    $self->{force}      = $opts{f} && @ARGV ? $opts{f} : '';
    $self->{noop}       = $opts{n} || '';
    $self->{quiet}      = $opts{q} || '';
    $self->{test_mode}  = $opts{t} || '';
    $self->{update}     = $opts{u} || '';

    if ($self->{debug}) {
        printf "+ brief: %s\n",  $self->{brief};
        printf "+ debug: %s\n",  $self->{debug};
        printf "+ force: %s\n",  $self->{force};
        printf "+ noop: %s\n",   $self->{noop};
        printf "+ quiet: %s\n",  $self->{quiet};
        printf "+ test: %s\n",   $self->{test_mode};
        printf "+ update: %s\n", $self->{update};
    }

    return @ARGV;
}

# Find outstanding deltas
sub find_deltas
{
    my $self = shift;
    my @delta = @_;

    @delta = <20*.sql> unless @delta;
    unless (@delta) {
      $self->_disconnect;
      print ("No deltas found (pattern '20*sql') - exiting.\n");
      exit 0;
    }
    print "+ candidate deltas: " . join(',', @delta) . "\n" if $self->{debug};

    my @outstanding = ();
    $self->{tag} = {};
    $self->{file} = {};
    $self->{tables} = {};
    $self->{insert} = {};
    for my $d (@delta) {
        my $fh = IO::File->new($d, 'r') or $self->_die("cannot open delta '$d': $!");
        my $file;
        if (defined $fh) {
            local $/ = undef;
            $file = <$fh>;
            undef $fh;
        }
        if ($file =~ m!^(/\*|--).*?tag:\s*(\S+)!m) {
            my $tag = $2;
            if ($file =~ m!^(/\*|--).*?tables?:\s*(.*)!m) {
                my $row = $self->{dbh}->selectrow_hashref(qq(
                    select * from delta where delta_id = '$tag'
                ));
                if ($self->{debug}) {
                    print "+ delta '$d' / '$tag' ";
                    print "NOT " if ! ref $row;
                    print "found\n";
                }
                if (! ref $row || $self->{force}) {
                    my $table = $2;
                    $table =~ s!\s+\*/.*!!;
                    push @outstanding, $d;
                    $self->{tag}->{$d} = $tag;
                    $file =~ s/^--[^\n]*\n/\n/mg;
                    $file =~ s/^\s*\n+//;
                    $file =~ s/\n\s*\n+/\n/g;
                    $self->{file}->{$d} = $file;
                    $self->{tables}->{$d} = $table;
                    $self->{insert}->{$d} = 1 unless ref $row;
                }
            }
            else {
                $self->_die("No table line found in file '$d'");
            }
        }
        else {
            $self->_die("No tag found in file '$d'");
        }
    }

    return @outstanding;
}

# Apply the given deltas to the database
sub apply_deltas
{
    my $self = shift;
    my $dbh = $self->{dbh};

    for my $d (@_) {
        my $delta = $self->{file}->{$d};
        # Escape semicolons inside single-quoted strings
        my @bits = split /(?<!\\)'/, $delta;
        printf "+ delta split into %d bits\n", scalar(@bits) if $self->{debug};
        for (my $i = 1; $i <= $#bits; $i += 2) {
            print "+ checking string '$bits[$i]' for semi-colons\n" if $self->{debug};
            $bits[$i] =~ s/(?<!\\);/\\;/g;
            print "+ munged string: '$bits[$i]'\n" if $self->{debug};
        }
        $delta = join("'", @bits);
#       do {} while $delta =~ s/\G([^']*'[^']*)(?<!\\);([^']*')/$1\\;$2/gsm;
        # Split each file into a set of statements on (non-escaped) semicolons
        my @stmt = split /(?<!\\);/, $delta;
        # Skip everything after the last semicolon
        pop @stmt if @stmt > 1;
        $self->{stmt}->{$d} = \@stmt if $self->{test_mode};
        printf "+ [%s] %d statement(s) found:\n---\n%s\n---\n", 
            $d, scalar(@stmt), join("\n---", @stmt) if $self->{debug};

        # Execute the statements 
        for (my $i = 0; $i <= $#stmt; $i++) {
            print "+ executing stmt $i ... " if $self->{debug};
            # Unescape semicolons escaped above
            $stmt[$i] =~ s/\\;/;/g;
            my $st = $self->filter_statement( $stmt[$i] );
            if ($self->{noop}) {
              print "\n\n[NOOP]\n$st\n\n";
            }
            else {
              $dbh->do($st)
                or $self->_die("[$d] update failed: " . $dbh->errstr . "\ndoing: $st\n");
            }
            print "+ done\n" if $self->{debug} && ! $self->{noop};
        }

        # Update the delta table
        if ($self->{insert}->{$d} && ! $self->{noop}) {
            print "+ inserting delta record ... " if $self->{debug};
            my $sth = $dbh->prepare(qq(
                insert into delta (delta_id, delta_tables) values (?, ?)
            ));
            $self->_die("delta insert prepare failed: " . $dbh->errstr) 
              unless $sth;

            $sth->execute(
                $self->{tag}->{$d}, 
                $self->{tables}->{$d},
            ) or $self->_die("delta insert execute failed: " . $dbh->errstr . 
                "\n");

            print "done\n" if $self->{debug};
        }
    }

    print "All done.\n" unless $self->{quiet};

    return @_;
}

# Main method
sub run
{
    my $class = shift;
    my $self = bless {}, $class;

    # Parse arguments
    my @args = @_;
    unless (@args) {
      @args = @ARGV;
      @ARGV = ();
    }
    @args = $self->parse_args(@args);

    # Connect to db
    $self->{dbh} = $self->connect;
    die "invalid dbh handle" unless ref $self->{dbh};

    # Find outstanding deltas
    my @outstanding = $self->find_deltas(@args);
    if (! @outstanding) {
        if (@args) {
            print "$_ already applied.\n" for @args;
        }
        else {
            print "No outstanding deltas found.\n";
        }
        $self->_disconnect;
        return 0;
    }

    my @return = @outstanding;

    unless ($self->{quiet}) {
      print $self->{update} ? "Applying deltas:\n" : "Outstanding deltas:\n" unless $self->{brief};
      foreach (@outstanding) {
        print $self->{brief} ? '' : '  ';
        print "$_\n";
      }
    }

    @return = $self->apply_deltas(@outstanding) if $self->{update};

    $self->_disconnect;

    return wantarray ? ( scalar(@return), \@return ) : scalar (@return);
}

1;

__END__

=head1 NAME

DBIx::Delta - a module to apply outstanding database deltas (files 
containing arbitrary sql statements) to a database instance.

=head1 SYNOPSIS

    # Must be used via a subclass providing a db connection e.g.
    package Foo::Delta;
    use DBI;
    use base qw(DBIx::Delta);
    sub connect { 
        DBI->connect('dbi:SQLite:dbname=foo.db','','');
    }
    1;

    # Then in a delta run script (e.g. delta.pl):
    use Foo::Delta;
    Foo::Delta->run;

    # And requires a 'delta' table to track changes (see below)

    # Then to check for deltas that have not been applied
    ./delta.pl 
    # And to apply those deltas and update the database
    ./delta.pl -u


=head1 DESCRIPTION

DBIx::Delta is a module used to apply database deltas (changes) to a 
database instance. It is intended for use in maintaining multiple 
database schemas in sync e.g. you create deltas on your development 
database instance, and subsequently apply those deltas to your test
instance, and then finally to production. 

It is simple and only requires DBI/DBD for your database connectivity.

=head2 DELTAS

Deltas are files containing arbitrary sql statements, identified by a 
unique per-file tag. DBIx::Delta tracks which delta files have been 
applied by means of a special 'delta' table, and will apply (execute) 
any deltas that are outstanding against your database.

DBIx::Delta files contain series of SQL statements to apply in the 
database, and must also contain two additional metadata fields within 
SQL comments, having the form 'key: value'. The first is a unique 
identifier to be used as the delta_id, whose key may be anything ending
in 'tag' e.g. 'tag', 'delta-tag' etc.; the second is a description of the 
tables affected by this delta, whose key should be 'table' or 'tables'
'tables' e.g.

    -- tag: 666da042-676e-4026-9862-6e7e0a1d3fa0
    -- table: listing

or:

    -- delta-tag: 7ae84801-e323-4a6f-984e-6de4f939a12c
    -- tables: emp, emp_address

DBIx::Delta tracks uses the tag identifier to track which delta files 
have been applied by inserting a record into a special 'delta' table 
that must exist in the database, with the following structure (SQLite):

    -- Create/bootstrap delta table (SQLite)
    create table delta (
      delta_id 	        varchar primary key,
      delta_tables	    varchar not null,
      delta_apply_ts	timestamp default CURRENT_TIMESTAMP
    );

and similarly, for mysql or postgresql:

    -- Create/bootstrap delta table (mysql/postgresql)
    create table delta (
        delta_id        varchar(100) primary key,
        delta_tables    varchar(255) not null,
        delta_apply_ts  timestamp default now()
    );


=head2 USAGE

    # Must be used via a subclass providing a db connection e.g.
    package Foo::Delta;
    use DBI;
    use base qw(DBIx::Delta);
    sub connect { 
        DBI->connect('dbi:SQLite:dbname=foo.db','','');
    }
    1;

    # And then ...
    perl -MFoo::Delta -le 'Foo::Delta->run'


=head1 AUTHOR

Gavin Carr <gavin@openfusion.com.au>

=head1 LICENCE

Copyright 2005-2008, Gavin Carr.

This program is free software. You may copy or redistribute it under the 
same terms as perl itself.

=cut

