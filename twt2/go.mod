module palecci.cz/twt2

go 1.23.0

toolchain go1.23.11

replace palecci.cz/twt2 => ../twt2

require (
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.10.0
	golang.org/x/crypto v0.41.0
	google.golang.org/protobuf v1.36.7
	palecci.cz/twtproto v0.0.0-00010101000000-000000000000
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace palecci.cz/twtproto => ../twtproto
