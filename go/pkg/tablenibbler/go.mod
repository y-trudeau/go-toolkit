module github.com/y-trudeau/go-toolkit/go/pkg/tablenibbler

go 1.24.3

require (
	github.com/y-trudeau/go-toolkit/go/pkg/quoter v0.0.0
	github.com/y-trudeau/go-toolkit/go/pkg/tableparser v0.0.0
    github.com/y-trudeau/go-toolkit/go/pkg/debug v0.0.0 // indirect
)


replace (
	github.com/y-trudeau/go-toolkit/go/pkg/debug => ../debug
	github.com/y-trudeau/go-toolkit/go/pkg/quoter => ../quoter
	github.com/y-trudeau/go-toolkit/go/pkg/tableparser => ../tableparser
)
