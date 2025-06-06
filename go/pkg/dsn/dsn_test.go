package dsn

import (
	"testing"

	"github.com/y-trudeau/go-toolkit/go/pkg/dsn"
)

func TestValidate(t *testing.T) {
	{
		// Non existent param
		v := "z=aef"
		err := dsn.Validate(v)
		if err.Error() != "Unknown parameter 'z'" {
			t.Errorf("Invalid paramater not recognized correctly '%v'", err.Error())
		}
	}
	{
		// no '='
		v := "L"
		err := dsn.Validate(v)
		if err.Error() != "Parameter 'L' is missing an '='" {
			t.Errorf("Invalid detection of missing '=' in dsn parameter, got: '%v'", err.Error())
		}
	}
	{
		// alpha port
		v := "P=abc"
		err := dsn.Validate(v)
		if err.Error() != "Port value must be composed of digits, received: 'abc'" {
			t.Errorf("Invalid detection of non-digits in the port value, got: '%v'", err.Error())
		}
	}
	{
		// Negative port
		v := "P=-1"
		err := dsn.Validate(v)
		if err.Error() != "Port value must be composed of digits, received: '-1'" {
			t.Errorf("Invalid detection of negative port value, got: '%v'", err.Error())
		}
	}
	{
		// Port too large
		v := "P=70000"
		err := dsn.Validate(v)
		if err.Error() != "Port value should be between 1 and 65535, value submitted was '70000'" {
			t.Errorf("Invalid detection of too large port value, got: '%v'", err.Error())
		}

	}
	{
		// L not 0 or 1
		v := "L=2"
		err := dsn.Validate(v)
		if err.Error() != "Local value must be 0 or 1, received: '2'" {
			t.Errorf("Invalid detection of wrong local value, got: '%v'", err.Error())
		}

	}

}

func TestInit(t *testing.T) {
	{
		// Validate the default value
		d := Dsn{}
		d.init()

		if d.Charset != "utf8" {
			t.Errorf("init didn't set the correct default Charset, got '%v'", d.Charset)
		}
		if d.Database != "" {
			t.Errorf("init didn't set the correct default Database, got '%v'", d.Database)
		}
		if d.DefaultsFile != "" {
			t.Errorf("init didn't set the correct default DefaultsFile, got '%v'", d.DefaultsFile)
		}
		if d.Host != "" {
			t.Errorf("init didn't set the correct default Host, got '%v'", d.Host)
		}
		if d.Local != false {
			t.Errorf("init didn't set the correct default Local, got '%v'", d.Local)
		}
		if d.Password != "" {
			t.Errorf("init didn't set the correct default Password, got '%v'", d.Password)
		}
		if d.Port != 3306 {
			t.Errorf("init didn't set the correct default Port, got '%v'", d.Port)
		}
		if d.Socket != "" {
			t.Errorf("init didn't set the correct default Socket, got '%v'", d.Socket)
		}
		if d.User != "" {
			t.Errorf("init didn't set the correct default User, got '%v'", d.User)
		}
	}

}

func TestParse(t *testing.T) {
	{
		// Test Charset parsing
		d := Dsn{}
		d.Parse("A=utf8mb4")

		if d.Charset != "utf8mb4" {
			t.Errorf("Parse didn't set the correct Charset, got '%v'", d.Charset)
		}
	}
	{
		// Test Database parsing
		d := Dsn{}
		d.Parse("D=testing")

		if d.Database != "testing" {
			t.Errorf("Parse didn't set the correct Database, got '%v'", d.Database)
		}
	}
	{
		// Test DefaultsFile parsing
		d := Dsn{}
		d.Parse("F=/testing/my.cnf")

		if d.DefaultsFile != "/testing/my.cnf" {
			t.Errorf("Parse didn't set the correct DefaultsFile, got '%v'", d.DefaultsFile)
		}
	}
	{
		// Test Host parsing
		d := Dsn{}
		d.Parse("h=127.0.0.1")

		if d.Host != "127.0.0.1" {
			t.Errorf("Parse didn't set the correct Host, got '%v'", d.Host)
		}
	}
	{
		// Test Local parsing
		d := Dsn{}
		d.Parse("L=1")

		if !d.Local {
			t.Errorf("Parse didn't set the correct Local, got '%v'", d.Local)
		}
	}
	{
		// Test Password parsing
		d := Dsn{}
		d.Parse("p=mypass")

		if d.Password != "mypass" {
			t.Errorf("Parse didn't set the correct Password, got '%v'", d.Password)
		}
	}
	{
		// Test Port parsing
		d := Dsn{}
		d.Parse("P=6033")

		if d.Port != 6033 {
			t.Errorf("Parse didn't set the correct Port, got '%v'", d.Port)
		}
	}
	{
		// Test Socket parsing
		d := Dsn{}
		d.Parse("S=/tmp/mysock.sock")

		if d.Socket != "/tmp/mysock.sock" {
			t.Errorf("Parse didn't set the correct Socket, got '%v'", d.Socket)
		}
	}
	{
		// Test User parsing
		d := Dsn{}
		d.Parse("u=myuser")

		if d.User != "myuser" {
			t.Errorf("Parse didn't set the correct myuser, got '%v'", d.User)
		}
	}
	{
		// Test multiple params
		d := Dsn{}
		d.Parse("P=6033,u=myuser,L=1,S=/tmp/mysock.sock")

		if !(d.User == "myuser" && d.Port == 6033 && d.Local && d.Socket == "/tmp/mysock.sock") {
			t.Errorf("Parse didn't processed correctly multiple parameters, got Port: '%v', User:'%v', Local: '%v', Socket: '%v'", d.Port, d.User, d.Local, d.Socket)
		}
	}

}
