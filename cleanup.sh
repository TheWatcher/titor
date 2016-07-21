#/bin/bash

# A simple convenience script to do background removal of the contents of the
# cleanup directory specified.

CLEANDIR=$1

# Run rm in a way that only has it working while the system is otherwise idle
ionice -c 3 rm -rf "$CLEANDIR/*"