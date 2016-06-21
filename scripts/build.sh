export GIT_COMMIT_SHORT=`git rev-parse --short HEAD`
echo "building sonicd $GIT_COMMIT_SHORT"
sbt clean assembly && \
  docker build -t ernestrc/sonicd:$GIT_COMMIT_SHORT .
