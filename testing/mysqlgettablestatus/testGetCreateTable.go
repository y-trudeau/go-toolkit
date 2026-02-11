package main

import (
    "fmt"
    "database/sql"
    "os"
    "regexp"

    "github.com/y-trudeau/go-toolkit/go/pkg/debug"
    "github.com/y-trudeau/go-toolkit/go/pkg/quoter"
    "github.com/y-trudeau/go-toolkit/go/pkg/dsn"
)

func GetCreateTable (dbh *sql.DB, db string, table string) (string,error) {

    // Make sure the SQL_MODE is set correctly
    var sql string 
    sql = "/*!40101 SET @OLD_SQL_MODE := @@SQL_MODE, @@SQL_MODE := ''," +
            "@OLD_QUOTE := @@SQL_QUOTE_SHOW_CREATE," +
            "@@SQL_QUOTE_SHOW_CREATE := 1 */"
    debug.Printvar("Setting sql mode: ", sql)
    rs, err := dbh.Query(sql)
    if err != nil {
        return "", fmt.Errorf("Unable to set the required SQL_MODE: %v", err)
    }
    rs.Close()

    // run the show create table
    sql = "SHOW CREATE TABLE " + quoter.Backtick([]string{db, table}) + ";"
    debug.Printvar("Show create table sql: ", sql)
    var dummy string  // For the table name column
    var ct string
    err = dbh.QueryRow(sql).Scan(&dummy, &ct)
    if err != nil {
        return "", fmt.Errorf("Unable to get the show create table result: %v", err)
    }
    debug.Printvar("Query return ct: ", ct)

    // Make sure the SQL_MODE is unset correctly
    sql = "/*!40101 SET @@SQL_MODE := @OLD_SQL_MODE," +
           "@@SQL_QUOTE_SHOW_CREATE := @OLD_QUOTE */"
    debug.Printvar("Unsetting sql mode: ", sql)
    rs, err = dbh.Query(sql)
    if err != nil {
        return "", fmt.Errorf("Unable to unset the SQL_MODE: %v", err)
    }
    defer rs.Close()

    re := regexp.MustCompile(`(?i)create (?:table|view)`)
    if !re.MatchString(ct) {
        return "", fmt.Errorf("No 'Create Table' or 'Create View' in result set: %v", ct)
    }

    return ct, nil
}


func main() {

    var d dsn.Dsn

    errp := d.Parse("h=10.0.3.10,u=root,p=root")
    if errp != nil {
        fmt.Println("Error Parsing the DSN string");
        os.Exit(1)
    }


    db, err := d.Getconn()
    if err != nil {
        fmt.Println("Error connecting to database: " + err.Error());
        os.Exit(1)
    }

    ct, errct := GetCreateTable(db,"dojo","Recus")
    if errct != nil {
        fmt.Println("Error getting Create table: ", errct)
    }

    fmt.Println(ct)
}
