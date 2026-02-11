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
   list of parameters like: 
   `h=host1,P=3306,u=bob,v="innodb_lock_wait_timeout=5,long_query_time=0,log_slow_verbosity=\"microtime,innodb\""`

   The possible parameters are:

   A  Default character set for the connection (SET NAMES).

   b  Disable binlog on the connection (SQL_LOG_BIN = 0), true/false, default is false 
      (uses strconv.ParseBool)

   D  Default database to use when connecting. Tools may USE a different databases while running.

   F  Defaults file for the MySQL client library

   h  MySQL hostname or IP address to connect to.

   L  Explicitly enable LOAD DATA LOCAL INFILE.

   p  MySQL password to use when connecting.

   P  Port number to use for the connection.

   S  MySQL socket file to use for the connection (on Unix systems).

   s  Use SSL, true/false, default is true (uses strconv.ParseBool) 

   t  Target table to work on

   u  MySQL username to use when connecting, if not current system user.

   v  MySQL session variables to set when a connection is created

   x  Extra Go MySQL driver parameter. No validation. SSL already set by 's'.
      By default, only "parseTime=true" is set.
      See https://github.com/go-sql-driver/mysql?tab=readme-ov-file#parameters

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

    "github.com/y-trudeau/go-toolkit/go/pkg/debug"
)

type Dsn struct {
    SkipBinlog   string
    Database     string
    Host         string
    Password     string
    Port         uint16
    Setvars      string
    Socket       string
    Ssl          bool
    Table        string
    User         string
    Extra        string
    Dbh          *sql.DB
}

// SplitAtCommas split s at commas, ignoring commas in strings.
// From Chetan Kumar on stackoverflow
func SplitAtCommas(s string) []string {
    res := []string{}
    var beg int
    var inString bool

    for i := 0; i < len(s); i++ {
        if s[i] == ',' && !inString {
            res = append(res, s[beg:i])
            beg = i+1
        } else if s[i] == '"' {
            if !inString {
                inString = true
            } else if i > 0 && s[i-1] != '\\' {
                inString = false
            }
        }
    }
    return append(res, s[beg:])
}

func Validate(dsnValue string) error {
	params := SplitAtCommas(dsnValue)

	re := regexp.MustCompile(`^(A|b|D|F|h|L|p|P|s|S|u|v|x){1}$`)

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
    D.SkipBinlog = false
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
    D.Extra = "parseTime=true"
}

func (D *Dsn) Parse(dsnValue string) error {
	err := Validate(dsnValue)
	D.init()

	if err != nil {
		return err
	}

	// From here, the format is clean and expected
	params := SplitAtCommas(dsnValue)
	for i := 0; i < len(params); i++ {
		// we now split around '='
		pSplit := strings.Split(params[i], "=")
        debug.Print("Parsing :" + pSplit[0] + " = " + pSplit[1])

		switch pSplit[0] {
		case "A":
			D.Charset = pSplit[1]
		case "b":
            var err error
            D.SkipBinlog, err = strconv.ParseBool(pSplit[1])
            if err != nil {
                 return fmt.Errorf("Failed to parse '%v' as boolean for parameter 'b'", pSplit[1])
            }
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
        case "v":
            // must be parsed by "`([a-z0-9_]*)=('[a-z0-9_\-,= ]*'|[a-z0-9]*),?`gm"
            if len(D.SetVars) > 0 {
                D.SetVars = D.SetVars + "," + strings.Replace(pSplit[1],"\"",'"',-1)
            }

        case "s":
            var err error
            D.Ssl, err = strconv.ParseBool(pSplit[1])
            if err != nil {
                 return fmt.Errorf("Failed to parse '%v' as boolean for parameter 's'", pSplit[1])
            }
		case "t":
			D.Table = pSplit[1]
		case "u":
			D.User = pSplit[1]
		case "x":
			D.Extra = pSplit[1]
		}
	}
	return nil
}

//func (D *Dsn) setVars(vars string) error {
//    // Very crude validation, just checking if there is an '='
//    if strings.Count(vars, "=") == 0 {
//        return fmt.Errorf("The variable provided: '%v' is missing an '='", vars)
//    }
//    D.Setvars = vars
//    return nil
//}

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

    // Process the variables after
    if D.Ssl || len(D.Extra) > 0 {
        // one or both settings are enabled
        Uri = Uri + "?"

        if D.Ssl {
            Uri = Uri + "allowFallbackToPlaintext=false,tls=true"
        }

        if D.Ssl && len(D.Extra) > 0 {
            Uri = Uri + ","
        }

        if len(D.Extra) > 0 {
            Uri = Uri + D.Extra
        }
    }
    debug.Print("Generated Uri = '" + Uri + "'")
    return Uri
}

func (D *Dsn) Getconn() (*sql.DB, error) {
    var err error
    if D.Dbh == nil {
        debug.Print("Creating a new connection to the database")
        D.Dbh, err = sql.Open("mysql", D.genUri())
        if err != nil {
            return nil, err
        }
        debug.Print("Connected to the database")
        D.Dbh.SetMaxOpenConns(1)

        if D.SkipBinlog {
            debug.Print("Skipping binary logging")
            _, err = D.Dbh.Query("SET SQL_LOG_BIN = 0;")
            if err != nil {
                return nil, err
            }
        }
        vars := strings.Split(D.Setvars, ",")
        for i := 0; i < len(vars)-1; i++ {
            debug.Print("Setting variable '" + vars[i] + "'")
            _, err = D.Dbh.Query("SET " + vars[i] + ";")
            if err != nil {
                return nil, err
            }
        }
    } else {
        debug.Print("Reusing a connection to the database")
        if err = D.Dbh.Ping(); err != nil {
            debug.Print("Connection to the database is down, reconnecting")
            D.Dbh.Close()
            D.Dbh, err = sql.Open("mysql", D.genUri())
            if err != nil {
                return nil, err
            }
            debug.Print("Connected to the database")
            // defer D.Dbh.Close()
            D.Dbh.SetMaxOpenConns(1)

            vars := strings.Split(D.Setvars, ",")
            for i := 0; i < len(vars)-1; i++ {
                debug.Print("Setting variable '" + vars[i] + "'")
                _, err = D.Dbh.Query("SET " + vars[i] + ";")
                if err != nil {
                    return nil, err
                }
            }
        }
    }
    return D.Dbh, nil
}

