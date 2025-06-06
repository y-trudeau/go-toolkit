package version

import (
	"testing"
)

func TestValidate(t *testing.T) {
	{
		// Invalid major version alpha
		v := "8a.0.30"
		valid := Validate(v)
		if valid {
			t.Errorf("Validation returned true for '%s'", v)
		}
	}
	{
		// Invalid minor mid version alpha
		v := "8.0c.30"
		valid := Validate(v)
		if valid {
			t.Errorf("Validation returned true for '%s'", v)
		}
	}
	{
		// Invalid minor last version alpha
		v := "8.0.30d"
		valid := Validate(v)
		if valid {
			t.Errorf("Validation returned true for '%s'", v)
		}
	}
	{
		// Too old version
		v := "4.0.30"
		valid := Validate(v)
		if valid {
			t.Errorf("Validation returned true for '%s'", v)
		}
	}
	{
		// Too new version
		v := "9.0.30"
		valid := Validate(v)
		if valid {
			t.Errorf("Validation returned true for '%s'", v)
		}
	}
	{
		// Too new version
		v := "9.0.30"
		valid := Validate(v)
		if valid {
			t.Errorf("Validation returned true for '%s'", v)
		}
	}
}

func TestMajor(t *testing.T) {
	{
		v := "8.0.30-rel"
		maj, _ := Major(v)
		if maj != "8" {
			t.Errorf("Major returned '%s' for version '%s'", maj, v)
		}
	}
}

func TestMinor(t *testing.T) {
	{
		v := "8.0.30-rel"
		min, _ := Minor(v)
		if min != "0.30" {
			t.Errorf("Minor returned '%s' for '%s'", min, v)
		}
	}
}

func TestRelease(t *testing.T) {
	{
		v := "8.0.30-rel"
		rel, _ := Release(v)
		if rel != "-rel" {
			t.Errorf("Release returned '%s' for '%s'", rel, v)
		}
	}
}

func TestNormalize(t *testing.T) {
	{
		v := "8.0.30-rel"
		n, _ := Normalized(v)
		if n != "80030" {
			t.Errorf("Normalized returned '%s' for '%s'", n, v)
		}
	}
	{
		v := "8.0.9-rel"
		n, _ := Normalized(v)
		if n != "80009" {
			t.Errorf("Normalized returned '%s' for '%s'", n, v)
		}
	}

}

func TestCompare(t *testing.T) {
	{
		v1 := "8.0.30-rel"
		v2 := "5.7.55"
		c, _ := Compare(v1, v2)
		if c != 1 {
			t.Errorf("Compared returned %d for v1 = '%s' and v2 = '%s'", c, v1, v2)
		}
	}
	{
		v2 := "8.0.30-rel"
		v1 := "5.7.55"
		c, _ := Compare(v1, v2)
		if c != -1 {
			t.Errorf("Compared returned %d for v1 = '%s' and v2 = '%s'", c, v1, v2)
		}
	}
	{
		v2 := "8.0.30-rel"
		v1 := "8.0.30-rel1"
		c, _ := Compare(v1, v2)
		if c != 0 {
			t.Errorf("Compared returned %d for v1 = '%s' and v2 = '%s'", c, v1, v2)
		}
	}

}
