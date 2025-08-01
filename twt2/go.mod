module palecci.cz/twt2

go 1.23.0

toolchain go1.23.11

replace palecci.cz/twt2 => ../twt2

require (
	github.com/sirupsen/logrus v1.8.1
	golang.org/x/crypto v0.17.0
	google.golang.org/protobuf v1.27.1
	palecci.cz/twtproto v0.0.0-00010101000000-000000000000
)

require (
	github.com/google/go-cmp v0.6.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
)

replace palecci.cz/twtproto => ../twtproto
