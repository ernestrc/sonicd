#!/usr/bin/env bash

sbt <<HEREDOC
clean
project sonicd-spark
publishLocal
HEREDOC

sbt test
