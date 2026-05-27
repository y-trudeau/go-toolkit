package quoter_test

import (
	"testing"

	"github.com/y-trudeau/go-toolkit/go/pkg/quoter"
)

func TestBacktick(t *testing.T) {
	{
		// Empty slice
		r := quoter.Backtick([]string{})
		if r != "" {
			t.Errorf("Empty slice: expected '', got '%v'", r)
		}
	}
	{
		// Single value
		r := quoter.Backtick([]string{"a"})
		if r != "`a`" {
			t.Errorf("Single value: expected '`a`', got '%v'", r)
		}
	}
	{
		// Two values joined with dot
		r := quoter.Backtick([]string{"a", "b"})
		if r != "`a`.`b`" {
			t.Errorf("Two values: expected '`a`.`b`', got '%v'", r)
		}
	}
	{
		// Already-backticked value: backtick inside is doubled
		r := quoter.Backtick([]string{"`a`"})
		if r != "```a```" {
			t.Errorf("Already quoted: expected '```a```', got '%v'", r)
		}
	}
	{
		// Internal backtick is doubled
		r := quoter.Backtick([]string{"a`b"})
		if r != "`a``b`" {
			t.Errorf("Internal quote: expected '`a``b`', got '%v'", r)
		}
	}
	{
		// Values with internal spaces
		r := quoter.Backtick([]string{"my db", "my tbl"})
		if r != "`my db`.`my tbl`" {
			t.Errorf("Spaces: expected '`my db`.`my tbl`', got '%v'", r)
		}
	}
}
