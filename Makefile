
build:
	go build github.com/Bromles/epaxos-fixed/src/master
	go build github.com/Bromles/epaxos-fixed/src/server
	go build github.com/Bromles/epaxos-fixed/src/client


test:
	./bin/test.sh
