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
    "database/sql"
    "fmt"
    "os"
    "regexp"
    "strconv"
    "strings"
    _ "github.com/go-sql-driver/mysql"
)

type Dsn struct {
    Charset      string
    Database     string
    Host         string
    Password     string
    Port         uint16
    Setvars      string
    Socket       string
    Ssl          bool
    Table        string
    User         string
    Dbh          *sql.DB
}

func Validate(dsnValue string) error {
	params := strings.Split(dsnValue, ",")

	re := regexp.MustCompile(`^(A|D|h|L|p|P|S|V|t|u|s){1}$`)

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
	}
	return nil
}

func (D *Dsn) init() {
	D.Charset = "utf8mb4"
	D.Database = ""
	D.Host = ""
	D.Password = ""
	D.Port = 3306
	D.Socket = ""
    D.Ssl = true 
	D.Table = ""
	D.User = ""
    D.Dbh = nil
    D.Setvars = ""
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
		case "h":
			D.Host = pSplit[1]
		case "P":
			p, e := strconv.Atoi(pSplit[1])
	        if e != nil {
		        return fmt.Errorf("Port value '%v' does not convert to integer. %v",pSplit[1],e)
	        }
			D.Port = uint16(p)

		case "p":
			D.Password = pSplit[1]
		case "S":
			D.Socket = pSplit[1]
            // if there is a socket file, it has to exists and be available.
            if _, e := os.Stat(D.Socket); e != nil {
                 return fmt.Errorf("Socket file '%v' does not exist or is not accessible", D.Socket)
            }
        case "V":
            // need to parse the variables which could includes quotes, equals and commas :(
            // javascript: res = str.split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/)

        case "s":
            D.Ssl = true
		case "t":
			D.Table = pSplit[1]
		case "u":
			D.User = pSplit[1]
		}
	}
	return nil
}

func (D *Dsn) setVars(vars string) error {
    // Very crude validation, just checking if there is an '='
    if strings.Count(vars, "=") == 0 {
        return fmt.Errorf("The variable provided: '%v' is missing an '='", vars)
    }
    D.Setvars = vars
    return nil
}

func (D *Dsn) genUri() string {
    Uri := ""

    // First let's establish if the protocol used is tcp or socket
    if len(D.User) > 0 {
        Uri = D.User
        if len(D.Password) > 0 {
            Uri = Uri + ":" + D.Password
        }
    }
    Uri = Uri + "@"

    // Is a socket file defined?
    if len(D.Socket) > 0 {
        Uri = Uri + "unix(" + D.Socket + ")"
    } else {
        Uri = Uri + "tcp("
        if len(D.Host) > 0 {
            Uri = Uri + D.Host + ":" + strconv.Itoa(int(D.Port))
        } else {
            Uri = Uri + "localhost:" + strconv.Itoa(int(D.Port))
        }
    }
    Uri = Uri + ")/"

    if len(D.Database) > 0 {
        Uri = Uri + D.Database
    }

//    if len(D.Setvars) {
//        Uri = Uri + "?" + strings.Replace(strings.Replace(D.Setvars," ","",-1),",","&",-1)
//    }
// A => charset
// 
    return Uri
}

func (D *Dsn) Getconn() (*sql.DB, error) {
    var err error
    if D.Dbh == nil {
        D.Dbh, err = sql.Open("mysql", D.genUri())
        if err != nil {
            return nil, err
        }
        defer D.Dbh.Close()
        D.Dbh.SetMaxOpenConns(1)
        vars := strings.Split(D.Setvars, ",")
        for i := 0; i < len(vars); i++ {
            _, err = D.Dbh.Query("SET " + vars[i] + ";")
            if err != nil {
                return nil, err
            }
        }
    } else {
        if err = D.Dbh.Ping(); err != nil {
            D.Dbh.Close()
            D.Dbh, err = sql.Open("mysql", D.genUri())
            if err != nil {
                return nil, err
            }
            defer D.Dbh.Close()
            D.Dbh.SetMaxOpenConns(1)
        }
    }
    return D.Dbh, nil
}

