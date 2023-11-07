/*
   Copyright 2023, Yves Trudeau, Percona Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at


       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   Outfile writes rows to a file in SELECT INTO OUTFILE format. See
   https://dev.mysql.com/doc/refman/8.0/en/load-data.html  for more
   details

   This package parses DSN values.  A typical DSN value is a comma delimited
   list of parameters like: "h=host1,P=3306,u=bob"

   The possible parameters are:

   A  Default character set for the connection (SET NAMES).

   D  Default database to use when connecting. Tools may USE a different databases while running.

   F  Defaults file for the MySQL client library

   h  MySQL hostname or IP address to connect to.

   L  Explicitly enable LOAD DATA LOCAL INFILE.

   p  MySQL password to use when connecting.

   P  Port number to use for the connection.

   S  MySQL socket file to use for the connection (on Unix systems).

   u  MySQL username to use when connecting, if not current system user.

*/

package dsn

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type Dsn struct {
	Charset      string
	Database     string
	DefaultsFile string
	Host         string
	Local        bool
	Password     string
	Port         uint16
	Socket       string
	User         string
}

func Validate(dsnValue string) error {
	params := strings.Split(dsnValue, ",")

	re := regexp.MustCompile(`^(A|D|F|h|L|p|P|S|u){1}$`)

	for i := 0; i < len(params); i++ {
		// Each parameter must have an '=' sign (maybe more than one but al least one)
		if strings.Count(params[i], "=") == 0 {
			return fmt.Errorf("Parameter '%v' is missing an '='", params[i])
		}

		// we now split around '='
		pSplit := strings.Split(params[i], "=")

		// Test if within the available parameters
		if !re.MatchString(pSplit[0]) {
			return fmt.Errorf("Unknown parameter '%v'", pSplit[0])
		}

		// Testing if port is numeric
		if pSplit[0] == "P" {
			rep := regexp.MustCompile(`^[[:digit:]]*$`)

			if !rep.MatchString(pSplit[1]) {
				return fmt.Errorf("Port value must be composed of digits, received: '%v'", pSplit[1])
			}

			port, _ := strconv.Atoi(pSplit[1])
			if port < 1 || port > 65535 {
				return fmt.Errorf("Port value should be between 1 and 65535, value submitted was '%v'", pSplit[1])
			}
		}

		// Testing if port is numeric
		if pSplit[0] == "L" {
			rep := regexp.MustCompile(`^(0|1){1}$`)

			if !rep.MatchString(pSplit[1]) {
				return fmt.Errorf("Local value must be 0 or 1, received: '%v'", pSplit[1])
			}
		}

	}
	return nil
}

func (D *Dsn) init() {
	D.Charset = "utf8"
	D.Database = ""
	D.DefaultsFile = ""
	D.Host = ""
	D.Local = false
	D.Password = ""
	D.Port = 3306
	D.Socket = ""
	D.User = ""
}

func (D *Dsn) Parse(dsnValue string) error {
	err := Validate(dsnValue)
	D.init()

	if err != nil {
		return err
	}

	// From here, the format is clean and expected
	params := strings.Split(dsnValue, ",")
	for i := 0; i < len(params); i++ {
		// we now split around '='
		pSplit := strings.Split(params[i], "=")

		switch pSplit[0] {
		case "A":
			D.Charset = pSplit[1]
		case "D":
			D.Database = pSplit[1]
		case "F":
			D.DefaultsFile = pSplit[1]
		case "h":
			D.Host = pSplit[1]
		case "L":
			if pSplit[1] == "0" {
				D.Local = false
			} else {
				D.Local = true
			}

		case "P":
			p, _ := strconv.Atoi(pSplit[1])
			D.Port = uint16(p)

		case "p":
			D.Password = pSplit[1]
		case "S":
			D.Socket = pSplit[1]
		case "u":
			D.User = pSplit[1]
		}
	}
	return nil
}
