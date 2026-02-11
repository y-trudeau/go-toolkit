module tests/mysqlgettablestatus

go 1.24.3

replace github.com/y-trudeau/go-toolkit/go/pkg/dsn => /home/yves/src/go-toolkit/go/pkg/dsn
replace github.com/y-trudeau/go-toolkit/go/pkg/debug => /home/yves/src/go-toolkit/go/pkg/debug
replace github.com/y-trudeau/go-toolkit/go/pkg/quoter => /home/yves/src/go-toolkit/go/pkg/quoter

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/go-sql-driver/mysql v1.9.2 // indirect
	github.com/y-trudeau/go-toolkit/go/pkg/debug v0.0.0-20250625155247-5604a5fa587c // indirect
	github.com/y-trudeau/go-toolkit/go/pkg/dsn v0.0.0-20250625155247-5604a5fa587c // indirect
	github.com/y-trudeau/go-toolkit/go/pkg/quoter v0.0.0-20250625155247-5604a5fa587c // indirect
)
