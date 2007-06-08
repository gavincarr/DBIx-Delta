#
# Package to apply outstanding database deltas to a database instance.
#

package DBIx::Delta;

use Getopt::Std;
use File::Basename;
use POSIX qw(strftime);
use IO::File;
use DBI;
use strict;

use vars qw($VERSION);
$VERSION = 0.01;

# abstract connect() - should be overridden with a sub returning a valid $dbh
sub connect
{
    die "connect() is an abstract method that must be provided by a subclass";
}

sub _die
{
    my $self = shift;
    $self->{dbh}->disconnect;
    die join ' ', @_;
}

# Parse arguments
sub parse_args
{
    my $self = shift;
    push @ARGV, @_ if @_;

    my %opts = ();
    getopts('?dfhu',\%opts);

    if ($opts{'?'} || $opts{h}) {
      print "usage: " . basename($0) . " [-d] [-f] [-u] [<delta> ...]\n";
      exit 1;
    }

    $self->{debug} = $opts{d};
    $self->{force} = $opts{f} && @ARGV;
    $self->{update} = $opts{u};

    if ($self->{debug}) {
        printf "+ debug: %s\n", $self->{debug};
        printf "+ force: %s\n", $self->{force};
        printf "+ update: %s\n", $self->{update};
    }
}

# Find outstanding deltas
sub find_deltas
{
    my $self = shift;

    my @delta = @ARGV;
    @delta = <20*.sql> unless @delta;
    unless (@delta) {
      $self->{dbh}->disconnect;
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
        my $fh = IO::File->new($d, 'r');
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
        do {} while $delta =~ s/('.*)(?<!\\);(.*')/$1\\;$2/gsm;
        # Split each file into a set of statements on (non-escaped) semicolons
        my @stmt = split /(?<!\\);/, $delta;
        # Skip everything after the last semicolon
        pop @stmt if @stmt > 1;
        printf "+ %s: %d statement(s) found, updating ...\n", $d, scalar(@stmt)
            if $self->{debug};

        # Execute the statements 
        for (my $i = 0; $i <= $#stmt; $i++) {
            print "+ executing stmt $i ..." if $self->{debug};
            # Unescape semicolons escaped above
            $stmt[$i] =~ s/\\;/;/g;
            $dbh->do($stmt[$i])
              or $self->_die("[$d] update failed: " . $dbh->errstr . 
                "\ndoing: $stmt[$i]\n");
            print " done\n" if $self->{debug};
        }

        # Update the delta table
        if ($self->{insert}->{$d}) {
            print "+ inserting delta record ..." if $self->{debug};
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

            print " done\n" if $self->{debug};
        }
    }

    print "All done.\n";
}

# Main method
sub run
{
    my $class = shift;
    my $self = bless {}, $class;

    # Parse arguments
    $self->parse_args(@_);

    # Connect to db
    $self->{dbh} = $self->connect;
    die "invalid dbh handle" unless ref $self->{dbh};

    # Find outstanding deltas
    my @outstanding = $self->find_deltas;
    if (! @outstanding) {
        if (@ARGV) {
            print "$_ already applied.\n" for @ARGV;
        }
        else {
            print "No outstanding deltas found.\n";
        }
        $self->{dbh}->disconnect;
        exit 1;
    }

    print $self->{update} ? "Applying deltas:\n" : "Outstanding deltas:\n";
    print "  $_\n" foreach @outstanding;

    $self->apply_deltas(@outstanding) if $self->{update};

    $self->{dbh}->disconnect;
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

    # And then ...
    perl -MFoo::Delta -le 'Foo::Delta->run'


=head1 DESCRIPTION

DBIx::Delta is a module used to apply database deltas (changes) to a 
database instance. Deltas are simply files containing arbitrary sql 
statements, containing by a unique per-file tag. DBIx::Delta tracks which
delta files have been applied by means of a special 'delta' table, and 
will apply (execute) any deltas that are outstanding against your 
database.

DBIx::Delta files contain series of SQL statements to apply in the 
database, and must also contain two additional metadata fields within 
SQL comments, having the form 'key: value'. The first is a unique 
identifier to be used as the delta_id, whose key may be anything ending
in 'tag' e.g. 'tag', 'arch-tag'; the second is a description of the 
tables affected by this delta, whose key should be 'table' or 'tables'
'tables' e.g.

    -- table: listing
    -- tag: 666da042-676e-4026-9862-6e7e0a1d3fa0

or:

    -- tables: emp, emp_address
    -- arch-tag: 7ae84801-e323-4a6f-984e-6de4f939a12c


DBIx::Delta tracks uses the tag identifier to track which delta files 
have been applied by inserting a record into a special 'delta' table 
that must exist in the database, with the following structure:

    -- Create/bootstrap delta table (SQLite)
    create table delta (
	delta_id 	varchar primary key,
	delta_tables	varchar not null,
	delta_apply_ts	timestamp default CURRENT_TIMESTAMP
    );

=head1 AUTHOR

Gavin Carr <gavin@openfusion.com.au>


=head1 COPYRIGHT

Copyright 2005-2006, Open Fusion Pty. Ltd. All Rights Reserved.

This program is free software. You may copy or redistribute it under the 
same terms as perl itself.

=cut

# arch-tag: a41f466d-7930-4d6e-89cf-a9e6f698eebd

