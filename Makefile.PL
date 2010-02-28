# Load the Module::Install bundled in ./inc/
use inc::Module::Install;

# Define metadata
name            'DBIx-Delta';
all_from        'Delta.pm';

# Specific dependencies
requires        'DBI'               => 0;
build_requires  'Test::More'        => '0.42';
build_requires  'Test::Deep'        => 0;
build_requires  'File::Path'        => '2.08';
#recommends      'Your::OtherModule' => '0.01';

#no_index        'directory'         => 'demos';

#install_script  'bin/myscript';

auto_install;
WriteAll;

