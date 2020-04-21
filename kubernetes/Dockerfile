FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# VoltDB ports
# See https://docs.voltdb.com/AdminGuide/adminserverports.php
#      int  rep  zk   http jmx  admin client
EXPOSE 3021 5555 7181 8080 9090 21211 21212

# create and populate ~/bin with appropriate executable.
COPY ./instance/deployment/ ./
