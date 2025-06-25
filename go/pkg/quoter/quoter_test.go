package quoter

import (
    "testing"

    "github.com/y-trudeau/go-toolkit/go/pkg/quoter"
)

func testBacktick(t *testing.T) {
    {
        // Empty array
        v := [...]string{}
        r := quoter.Backtick(v)
        if r != "" {
            t.Errorf("Failed to quote correctly and empty array '%v' into '%v'", v, r)
        }
    }
    {
        // simple quote
        v := [...]string{"a"}
        r := quoter.Backtick(v)
        if r != "`a`" {
            t.Errorf("Failed to quote correctly a simple value '%v' into '%v'", v, r)
        }
    }
    {
        // quote multi values
        v := [...]string{"a", "b"}
        r := quoter.Backtick(v)
        if r != "`a`.`b`" {
            t.Errorf("Failed to quote correctly two values '%v' into '%v'", v, r)
        }
    }
    {
        // already quoted 
        v := [...]string{"`a`"}
        r := quoter.Backtick(v)
        if r != "```a```" {
            t.Errorf("Failed to quote correctly an already quoted '%v' into '%v'", v, r)
        }
    }
    {
        // internal quote 
        v := [...]string{"a`b"}
        r := quoter.Backtick(v)
        if r != "`a``b`" {
            t.Errorf("Failed to quote correctly an internal quote '%v' into '%v'", v, r)
        }
    }
    {
        // values with internal spaces 
        v := [...]string{"my db", "my tbl"}
        r := quoter.Backtick(v)
        if r != "`my db`.`my tbl`" {
            t.Errorf("Failed to quote correctly values with spaces '%v' into '%v'", v, r)
        }
    }
}

//func testQuoteval(t *testing.T) {
//}

//func testSplitunbacktick(t *testing.T) {
//}

//func testEscapelike(t *testing.T) {
//}

//func testSerializelist(t *testing.T) {
//}

//func testDeserializelist(t *testing.T) {
//}

