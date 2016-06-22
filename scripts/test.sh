#!/usr/bin/env bash
export GIT_COMMIT_SHORT=`git rev-parse --short HEAD`;
export NODE_TLS_REJECT_UNAUTHORIZED=0;
export DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )";

run_tests() {
  (node $DIR/../examples/nodejs/example.js)
  local res=$?
  printf 'EXIT CODE: %d\n' "$res"
  return $res
}

$DIR/build.sh && \
  echo "Starting WS integration spec for $GIT_COMMIT_SHORT" && \
  NGINX_CONTAINER=$(docker run -v $DIR/sonicd-nginx.conf:/etc/nginx/nginx.conf:ro -v /etc/ssl/localcerts:/etc/ssl/localcerts:ro -d --net=host nginx) && \
  SONICD_CONTAINER=$(docker run -d -v $DIR:/etc/sonicd:ro -p 9111:9111 ernestrc/sonicd:$GIT_COMMIT_SHORT) && \
  sleep 5; \
  TEST_RESULT=$(run_tests) && \
  echo "finished WS integration spec for $GIT_COMMIT_SHORT. Cleaning..." && \
  docker rm -f $NGINX_CONTAINER; \
  docker rm -f $SONICD_CONTAINER; \
  echo "Result: $TEST_RESULT"

