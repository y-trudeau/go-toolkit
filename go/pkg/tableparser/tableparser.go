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

package tableparser

import (
    "database/sql"
    "fmt"
    "os"
    "regexp"
    "slices"
    "strconv"
    "strings"

    "github.com/y-trudeau/go-toolkit/go/pkg/debug"
    "github.com/y-trudeau/go-toolkit/go/pkg/quoter"
)

type KeyColInfo struct {
    name   string
    prefix int
    colddl string
}

type KeyInfo struct {
    name    string
    keyType string
    primary bool
    unique  bool
    cols    map[string]KeyColInfo
    keyddl  string
}

type FkInfo struct {
    name           string
    colnames       string
    cols           []string
    parenttb       string
    parentcolnames string
    parentcols     []string
    fkddl          string
}

type ColInfo struct {
    name       string
    pos        int
    dataType   string
    definition string
    nullable   bool
    generated  bool
    numeric    bool
    autoinc    bool
}

type TableInfo struct {
    name    string
    cols    map[string]ColInfo
    keys    map[string]KeyInfo
    engine  string
    charset string
    ddl     string
}

type TableStatusInfo struct {
    name           string
    engine         string
    version        uint
    rowFormat      string
    rows           uint64
    avgRowLength   uint16
    dataLength     uint64
    maxDataLength  uint64
    indexLength    uint64
    dataFree       uint64
    autoIncrement  uint64
    createTime     sql.NullTime
    updateTime     sql.NullTime
    checkTime      sql.NullTime
    collation      string
    checksum       sql.NullInt64
    createOptions  string
    comment        string
}

func GetCreateTable(dbh *sql.DB, db string, table string) (string, error) {
    // Make sure the SQL_MODE is set correctly
    var sqlStr string
    sqlStr = "/*!40101 SET @OLD_SQL_MODE := @@SQL_MODE, @@SQL_MODE := ''," +
        "@OLD_QUOTE := @@SQL_QUOTE_SHOW_CREATE," +
        "@@SQL_QUOTE_SHOW_CREATE := 1 */"
    debug.Printvar("Setting sql mode: ", sqlStr)
    rs, err := dbh.Query(sqlStr)
    if err != nil {
        return "", fmt.Errorf("Unable to set the required SQL_MODE: %v", err)
    }
    rs.Close()

    // run the show create table
    sqlStr = "SHOW CREATE TABLE " + quoter.Backtick([]string{db, table}) + ";"
    debug.Printvar("Show create table sql: ", sqlStr)
    var dummy string // For the table name column
    var ct string
    err = dbh.QueryRow(sqlStr).Scan(&dummy, &ct)
    if err != nil {
        return "", fmt.Errorf("Unable to get the show create table result: %v", err)
    }
    debug.Printvar("Query return ct: ", ct)

    // Make sure the SQL_MODE is unset correctly
    sqlStr = "/*!40101 SET @@SQL_MODE := @OLD_SQL_MODE," +
        "@@SQL_QUOTE_SHOW_CREATE := @OLD_QUOTE */"
    debug.Printvar("Unsetting sql mode: ", sqlStr)
    rs, err = dbh.Query(sqlStr)
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

// Parse the whole create table statement filling up a TableInfo struct.
func Parse(ddl string) (TableInfo, error) {
    // Sanitize the input
    if len(ddl) < 1 {
        return TableInfo{}, fmt.Errorf("Empty table definition provided")
    }

    // Are quotes used?
    re := regexp.MustCompile("(?i)CREATE (?:TEMPORARY )?TABLE `")
    if !re.MatchString(ddl) {
        return TableInfo{}, fmt.Errorf("tableparser doesn't handle CREATE TABLE without quoting.")
    }

    // Initialize the TableInfo struct
    ti := new(TableInfo)
    ti.ddl = ddl

    // Extract the table name
    re = regexp.MustCompile("CREATE (?:TEMPORARY )?TABLE `([^`]*)`")
    matches := re.FindAllStringSubmatch(ddl, -1)
    if len(matches) < 1 || len(matches[0][1]) < 1 {
        return TableInfo{}, fmt.Errorf("Couldn't extract the table name from: %v", ddl)
    }
    ti.name = matches[0][1]
    debug.Printvar("table name: ", ti.name)

    // Extract the table engine
    var err error
    ti.engine, err = Getengine(ddl)
    if err != nil {
        return TableInfo{}, fmt.Errorf("Couldn't extract the engine: %v", err)
    }

    // Extract the table default charset
    ti.charset, err = Getcharset(ddl)
    if err != nil {
        return TableInfo{}, fmt.Errorf("Couldn't extract the charset: %v", err)
    }

    // Process the columns using multiline
    re = regexp.MustCompile(`(?m)^\s\s` + "`" + `([^` + "`" + `]*)` + "`" + ` ([^\s]*) (.*)$`)
    colmatches := re.FindAllStringSubmatch(ddl, -1)
    ti.cols = make(map[string]ColInfo)
    for i, col := range colmatches {
        ci := new(ColInfo)
        ci.name = col[1]
        ci.pos = i + 1 // i will start at 0
        ci.dataType = col[2]
        ci.definition = col[0]
        ci.nullable = true
        if strings.Contains(ci.definition, "NOT NULL") {
            ci.nullable = false
        }

        ci.generated = false
        if strings.Contains(ci.definition, "GENERATED ALWAYS AS") {
            ci.generated = true
        }

        ci.numeric = false
        if strings.Contains(ci.dataType, "int") ||
            strings.Contains(ci.dataType, "float") ||
            strings.Contains(ci.dataType, "double") ||
            strings.Contains(ci.dataType, "decimal") ||
            strings.Contains(ci.dataType, "year") {

            ci.numeric = true
        }

        ci.autoinc = false
        if strings.Contains(col[3], "AUTO_INCREMENT") {
            ci.autoinc = true
        }

        ti.cols[ci.name] = *ci
    }

    ti.keys = GetKeys(ddl)

    return *ti, nil
}

// Parse columns from index defintions like: "`c`,`b`,`a`"
func parseIndexColumns(cols string) map[string]KeyColInfo {

    kcimap := make(map[string]KeyColInfo)
    el := strings.Split(cols, ",")

    // Separate the column from the prefix
    recolpref := regexp.MustCompile("(`[^(]*)(?:\\(([0-9]*)\\))?")

    for _, value := range el {
        matches := recolpref.FindAllStringSubmatch(value, -1)
        kci := new(KeyColInfo)
        // Remove leading and trailing backtick
        kci.name = strings.TrimLeft(strings.TrimRight(matches[0][1], "`"), "`")

        // Replace double backtick by single backtick
        kci.name = strings.ReplaceAll(kci.name, "``", "`")

        // Deal with the prefix if any
        kci.prefix = 0
        if len(matches[0][2]) > 0 {
            // The regex already validated it is an integer
            kci.prefix, _ = strconv.Atoi(matches[0][2])
        }

        kci.colddl = value

        kcimap[kci.name] = *kci
    }

    return kcimap
}

func GetKeys(ddl string) map[string]KeyInfo {

    kimap := make(map[string]KeyInfo)

    // First process the primary key
    re := regexp.MustCompile(`(?m)^  PRIMARY KEY \((.*)\),*$`)
    matches := re.FindAllStringSubmatch(ddl, -1)
    if len(matches) > 0 && len(matches[0]) > 1 {
        ki := new(KeyInfo)
        ki.name = "PRIMARY"
        ki.keyType = "BTREE"
        ki.primary = true
        ki.unique = true
        ki.cols = parseIndexColumns(matches[0][1])
        ki.keyddl = matches[0][0]
        kimap[ki.name] = *ki
    }

    re = regexp.MustCompile(`(?m)^  (FULLTEXT|SPATIAL|UNIQUE)*\s*KEY ` + "`" + `([^` + "`" + `]*)` + "`" + ` \((.*)\),*$`)
    matches = re.FindAllStringSubmatch(ddl, -1)
    for _, idxddl := range matches {
        ki := new(KeyInfo)

        ki.name = idxddl[2]

        ki.keyType = "BTREE"
        if strings.Contains(idxddl[1], "SPATIAL") {
            ki.keyType = "RTREE"
        }
        if strings.Contains(idxddl[1], "FULLTEXT") {
            ki.keyType = "TEXT"
        }

        ki.primary = false
        ki.unique = false
        if strings.Contains(idxddl[1], "UNIQUE") {
            ki.unique = true
        }

        ki.cols = parseIndexColumns(idxddl[3])
        ki.keyddl = idxddl[0]

        kimap[ki.name] = *ki
    }

    return kimap
}

// Returns the storage engine in use by the table.
// The actual create table statement is the only parameter
func Getengine(ddl string) (string, error) {
    re := regexp.MustCompile(`(?m)^\) ENGINE\=([^ ]*) .*$`)
    // matches: [[") ENGINE=InnoDB AUTO_INCREMENT=1999142 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci" "InnoDB"]]
    matches := re.FindAllStringSubmatch(ddl, -1)
    if len(matches) < 1 || len(matches[0][1]) < 1 {
        return "", fmt.Errorf("Could not determine the table Engine")
    }
    debug.Printvar("table engine: ", matches[0][1])
    return matches[0][1], nil
}

// Returns the default charset in use by the table.
// The actual create table statement is the only parameter
func Getcharset(ddl string) (string, error) {
    re := regexp.MustCompile(`(?m)DEFAULT CHARSET=([^\s]*)`)
    // matches: [["DEFAULT CHARSET=utf8mb4" "utf8mb4"]]
    matches := re.FindAllStringSubmatch(ddl, -1)
    if len(matches) < 1 || len(matches[0][1]) < 1 {
        return "", fmt.Errorf("Could not determine the table default charset")
    }
    debug.Printvar("table charset: ", matches[0][1])
    return matches[0][1], nil
}

// Sorts indexes in this order: PRIMARY, unique, non-nullable, any (shortest
// first, alphabetical).  Only BTREE indexes are considered.
// Could be replaced by mysql.innodb_index_stats but would need access
// to the database and a fresh analyze table.
func (tbl TableInfo) Sortindexes() []KeyInfo {
    // First, we need to create a slice of KeyInfo values
    kivalues := make([]KeyInfo, 0, len(tbl.keys)) // Pre-allocate capacity
    for _, v := range tbl.keys {
        // we are only considering btrees
        if v.keyType == "BTREE" {
            kivalues = append(kivalues, v)
        }
    }

    sortFunc := func(x, y KeyInfo) int {
        // pk wins and only one
        if x.name == "PRIMARY" {
            return -1
        }
        if y.name == "PRIMARY" {
            return 1
        }

        // From here we consider the unique attribute
        if x.unique && !y.unique {
            return -1
        }
        if y.unique && !x.unique {
            return 1
        }

        // At last we prefer index with more columns
        if len(x.cols) > len(y.cols) {
            return -1
        }
        if len(y.cols) > len(x.cols) {
            return 1
        }

        return 0
    }

    idxs := slices.SortedStableFunc(slices.Values(kivalues), sortFunc)
    debug.Printvar("Sorted indexes, best is first: ", idxs)
    return idxs
}

// Finds the 'best' index; if the user specifies one, dies if it's not in the
// table.
func (tbl TableInfo) Findbestindex(idx string) string {

    best := ""
    // Was there an index given?
    if len(idx) > 0 {
        // does it exists?
        if _, ok := tbl.keys[idx]; ok {
            // index exists
            best = idx
        } else {
            // Index doesn't exist, we bail out
            fmt.Fprintf(os.Stderr, "Index '%v' does not exist in table\n", idx)
            os.Exit(1)
        }
    } else {
        idxs := tbl.Sortindexes()
        best = idxs[0].name
    }

    debug.Printvar("Best index found is: ", best)
    return best
}

// Check if a table exists taking care of the potential lower case names.
func Checktable(dbh *sql.DB, db string, tbl string) (bool, error) {
    //select count(1) from tables where table_name = if(@@lower_case_table_names > 0,lower('ColumnS_pRiv'),'ColumnS_pRiv') and table_schema = 'mYsql' ;
    // Make sure the SQL_MODE is set correctly
    debug.Print("Checktable called with db: " + db + " and tbl: " + tbl)

    sqlStr := "select count(1) from information_schema.tables " +
        "where table_name = if(@@lower_case_table_names > 0,lower('" + tbl + "'),'" + tbl + "') " +
        "and table_schema = if(@@lower_case_table_names > 0,lower('" + db + "'),'" + db + "')"

    debug.Printvar("Checking if the table exists: ", sqlStr)
    var ct string
    var err error
    err = dbh.QueryRow(sqlStr).Scan(&ct)
    if err != nil {
        return false, fmt.Errorf("Unable to check if the table exists: %v", err)
    }

    debug.Printvar("Response from query: ", ct)
    if ct == "1" {
        return true, nil
    } else {
        return false, nil
    }
}

// Returns a map of FkInfo
func GetFks(ddl string) map[string]FkInfo {

    fkmap := make(map[string]FkInfo)

    // [["  CONSTRAINT `child_ibfk_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON DELETE CASCADE" "`child_ibfk_1`" "`parent_id`" "`parent`" "`id`"]]
    re := regexp.MustCompile(`(?m)^  CONSTRAINT (.*) FOREIGN KEY \((.*)\) REFERENCES (.*) \((.*)\).*$`)
    fkmatches := re.FindAllStringSubmatch(ddl, -1)
    for _, fkddl := range fkmatches {
        fki := new(FkInfo)

        fki.name = fkddl[1]
        fki.colnames = fkddl[2]
        fki.cols = strings.Split(fkddl[2], ",")
        fki.parenttb = fkddl[3]
        fki.parentcolnames = fkddl[4]
        fki.parentcols = strings.Split(fkddl[4], ",")
        fki.fkddl = fkddl[0]

        fkmap[fki.name] = *fki
    }

    return fkmap
}

//ignoring func remove_auto_increment has it doesn't seem to be used

// Returns table status info from db with optional like
func Gettablestatus(dbh *sql.DB, db string, like string) ([]TableStatusInfo, error) {
    sqlStr := "SHOW TABLE STATUS FROM " + quoter.Backtick([]string{db}) + " LIKE '" + like + "'"
    debug.Printvar("Show table status sql: ", sqlStr)
    rows, err := dbh.Query(sqlStr)
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
            &ts.collation, &ts.checksum, &ts.createOptions, &ts.comment); err != nil {
            return tableStatuses, err
        }
        tableStatuses = append(tableStatuses, ts)
    }
    if err = rows.Err(); err != nil {
        return tableStatuses, err
    }
    return tableStatuses, nil
}
