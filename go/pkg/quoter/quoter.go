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

   This package contains functions to quote/unquote fully qualified table
   names, columns and values.

*/

package quoter

import (
    "regexp"
    "strings"

    "github.com/y-trudeau/go-toolkit/go/pkg/debug"
)

// Backtick quotes values in backticks.
// Returns a strings of all backticked values separated by '.'
func Backtick(vals []string) string {
    re := regexp.MustCompile("`")
    retval := ""
    for _, el := range vals {
        retval = retval + "`" + re.ReplaceAll([]byte(el), []byte("``")) + "`."
    }
    return strings.TrimRight(retval,".")
}

// Quoteval quotes a value for use in a SQL statement.
// Examples: undef = "NULL" and empty string = ''
func Quoteval(val string, datatype string) string) {
    // is val NULL? 
    if val == nil {
        return "NULL"
    }

    if datatype != "char" {
        return val
    }

    return "'" + strings.ReplaceAll(strings.ReplaceAll(val,`\`,`\\`), "'", `\'`) + "'"

}

// Splitunbacktick a potentially backticked table name to 
// separate db and table names. If the database is not specified 
// in the table name, the defdb is used.
func Splitunbacktick (dbtbl string, defdb string) (string,string) {
    db := ""
    tbl := ""

    res := strings.Split(dbtbl,".")
    if len(res) == 2 {
        db = res[0]
        tbl = res[1]
    } else {
        db = defdb
        tbl = res[0]
    }
    // Remove trailing backtick
    db = strings.TrimRight(db,"`")
    tbl = strings.TrimRight(tbl,"`")
    // Remove leading backtick
    db = strings.TrimLeft(db,"`")
    tbl = strings.TrimLeft(tbl,"`")
    // Replace double backtick by single backtick
    db = strings.ReplaceAll(db,"``","`")
    tbl = strings.ReplaceAll(tbl,"``","`")

    return db, tbl
}

// Escapelike espaces SQL LIKE values, especially wildcard % and _.
func Escapelike (like string) string {
    return "'" + strings.ReplaceAll(strings.ReplaceAll(like,`%`,`\%`), "_", `\_`) + "'"
}

// Convert an array of string values to a string of quoted values
// separated by commas
func Serializelist(args []string) string {

    debug.Printvar("Serializing", args)
    result := ""
    for _, el := range args {
        result = result + strings.ReplaceAll(strings.ReplaceAll(el,`,`,`\,`), `\N`, `\\N`) + ","
    }

    if len(result) > 0 {
        result = strings.TrimRight(result,",")
    }

    debut.Printvar("Serialized", result)
    return result
}

func Deserializelist(list string) []string {
    debug.Printvar("Deserializing", list)

    var res []string
    re := regexp.MustCompile(`(?:[^\\,]|\\,)+`)
    matches := re.FindAllString(list, -1)

    for _, el := range matches {
        res = append(res,strings.ReplaceAll(strings.ReplaceAll(el,,`\,`,`,`), , `\\N`, `\N`))
    }

    debug.Printvar("Deserialed", res)
    return res
} 
