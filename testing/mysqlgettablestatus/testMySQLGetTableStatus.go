package main

import (
    "fmt"
    "database/sql"
    "os"

    "github.com/y-trudeau/go-toolkit/go/pkg/debug"
    "github.com/y-trudeau/go-toolkit/go/pkg/tableparser"
    "github.com/y-trudeau/go-toolkit/go/pkg/dsn"
)

type TableStatusInfo struct {
    name            string
    engine          string
    version         uint
    rowFormat       string
    rows            uint64
    avgRowLength    uint16
    dataLength      uint64
    maxDataLength   uint64
    indexLength     uint64
    dataFree        uint64
    autoIncrement   sql.NullInt64
    createTime      sql.NullTime
    updateTime      sql.NullTime
    checkTime       sql.NullTime
    collation       string
    checksum        sql.NullInt64
    createOptions   string
    comment         string
}

// Returns table status info from db with optional like
func LGetTableStatus(dbh *sql.DB) ([]TableStatusInfo, error) {
    sql := "SHOW TABLE STATUS FROM test LIKE '%'"
    debug.Printvar("Show table status sql: ", sql)
    rows, err := dbh.Query(sql)
    if err != nil {
        return nil, fmt.Errorf("Unable to get the show create table result: %v", err)
    }
    defer rows.Close()

    var tableStatuses []TableStatusInfo

    for rows.Next() {
        var ts TableStatusInfo
        if err := rows.Scan(&ts.name, &ts.engine, &ts.version, &ts.rowFormat, &ts.rows,
            &ts.avgRowLength, &ts.dataLength, &ts.maxDataLength, &ts.indexLength,
            &ts.dataFree, &ts.autoIncrement, &ts.createTime, &ts.updateTime, &ts.checkTime,
            &ts.collation, &ts.checksum, &ts.createOptions ,&ts.comment); err != nil {
            return tableStatuses, err
        }
        tableStatuses = append(tableStatuses, ts)
    }
    if rows.Err(); err != nil {
        return tableStatuses, err
    }
    return tableStatuses, nil
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

    tabletstatuses, errts := GetTableStatus(db)
    if errts != nil {
        fmt.Println("Error getting tables statuses: ", errts)
    }
    for _, t := range tabletstatuses {
        fmt.Printf("Table=%v, Rows=%v\n",t.name,t.rows)
    }
}
