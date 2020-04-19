#!/usr/bin/env sh

bin=$1

if [ -z $bin ]; then
  echo "binary is not defined"
  exit 1
fi

exec ./$bin
