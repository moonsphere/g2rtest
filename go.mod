module github.com/moonsphere/g2rtest

go 1.24.0

require github.com/ihciah/rust2go/mem-ring v0.0.0

require (
	github.com/edwingeng/deque/v2 v2.1.1 // indirect
	golang.org/x/sys v0.36.0 // indirect
)

replace github.com/ihciah/rust2go/mem-ring => ./gh/rust2go/mem-ring
