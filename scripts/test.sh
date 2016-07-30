#!/usr/bin/env bash
export GIT_COMMIT_SHORT=`git rev-parse --short HEAD`;
export NODE_TLS_REJECT_UNAUTHORIZED=0;
export DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )";
DOMAIN=sonicd.unstable.build

clean() {
  [[ -d "$DIR/certs" ]] && echo "skipping rm certs folder" || rm -rf $DIR/certs;
  [[ -z "$SONICD_CONTAINER" ]] && echo "skipping rm sonicd container" || docker rm -f $SONICD_CONTAINER;
  [[ -z "$NGINX_CONTAINER" ]] && echo "skipping rm nginx container" || docker rm -f $NGINX_CONTAINER;
}

create_certs() {
  mkdir $DIR/certs && \
    cd $DIR/certs && \
    $DIR/gencert.sh $DOMAIN && \
    cd $DIR && \
    echo 'generated temp certs'
}

test_exit() {
  if [ $? -ne 0 ]; then
    echo "exit status not 0: $1"
    exit 1
  fi
}

trap 'clean' EXIT;

$DIR/build.sh && echo "successfully ran unit tests and built xarxa6/sonicd:$GIT_COMMIT_SHORT";

[[ $? -ne 0 ]] && exit 1

echo "Starting WS integration spec for $GIT_COMMIT_SHORT";

create_certs;

NGINX_CONTAINER=$(docker run -v ${DIR}/nginx.conf:/etc/nginx/nginx.conf:ro -v ${DIR}/certs:/etc/ssl/localcerts:ro -d --net=host nginx);
test_exit $NGINX_CONTAINER
echo "deployed nginx ssl proxy container: $NGINX_CONTAINER";

SONICD_CONTAINER=$(docker run -d -v ${DIR}:/etc/sonicd:ro -p 9111:9111 xarxa6/sonicd:${GIT_COMMIT_SHORT});
test_exit $SONICD_CONTAINER
echo "deployed sonicd container: $SONICD_CONTAINER. starting tests in 5s..";
sleep 5;

cd $DIR/../lib/nodejs/ && npm install && 
  cd $DIR/../spec/ && npm install && npm test &&
  cd $DIR/../examples/nodejs && npm install && node example.js
