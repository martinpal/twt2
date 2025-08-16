module palecci.cz/tw2

go 1.23.0

toolchain go1.23.11

replace palecci.cz/twt2 => ../twt2

require (
	github.com/sirupsen/logrus v1.9.3
	palecci.cz/twt2 v0.0.0-00010101000000-000000000000
)

require (
	golang.org/x/crypto v0.41.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	google.golang.org/protobuf v1.36.7 // indirect
	palecci.cz/twtproto v0.0.0-00010101000000-000000000000 // indirect
)

replace palecci.cz/twtproto => ../twtproto
