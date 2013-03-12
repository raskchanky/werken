#!/bin/sh

# for starting a release
make rel
./rel/werken/bin/werken console

# for starting in development
# make
# erl -setcookie foo -pa apps/*/ebin -pa deps/*/ebin -s werken
