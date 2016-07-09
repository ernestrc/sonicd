export GIT_COMMIT_SHORT=`git rev-parse --short HEAD`
echo "building sonicd $GIT_COMMIT_SHORT"
sbt assembly && \
  docker build -t xarxa6/sonicd:$GIT_COMMIT_SHORT .
