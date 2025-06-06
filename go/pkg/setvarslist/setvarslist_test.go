package setvarslist

import (
	"testing"
    "github.com/y-trudeau/go-toolkit/go/pkg/setvarslist"
)

func TestGetvars(t *testing.T) {
	{
		// Empty
		v := ""
		vars := setvarslist.Getvars(v)
        if len(vars) != 0 {
            t.Errorf("Returned non-empty array")
        }
	}
	{
		// single value
		v := "A"
		vars := setvarslist.Getvars(v)
        if len(vars) != 1 {
            t.Errorf("Returned an array that do not have one element")
        }
        if vars[0] != "A" {
            t.Errorf("Incorrect value of the first element")
        }
	}
	{
		// multiple value
		v := "1,2,3,4"
		vars := setvarslist.Getvars(v)
        if len(vars) != 4 {
            t.Errorf("Returned an array that do not have four elements")
        }
	}
	{
		// Commas in value
		v := `A="B,C,D"`
		vars := setvarslist.Getvars(v)
        if len(vars) != 1 {
            t.Errorf("Returned an array that do not have one element")
        }
        if vars[0] != `A="B,C,D"` {
            t.Errorf("Incorrect value of the first element")
        }
	}
}

