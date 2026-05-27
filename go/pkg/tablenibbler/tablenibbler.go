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

   This package generates SQL statement metadata for iterating through a
   MySQL table in chunks (nibbling), used by tools like pt-archiver and
   pt-table-sync.
*/

package tablenibbler

import (
	"fmt"
	"strings"
    "strconv"

	"github.com/y-trudeau/go-toolkit/go/pkg/quoter"
	"github.com/y-trudeau/go-toolkit/go/pkg/tableparser"
    "github.com/y-trudeau/go-toolkit/go/pkg/debug"
)

// AscStmt holds metadata for a chunked ascending SELECT statement.
// Slice contains column ordinals (positions in Cols) for the ? placeholders in Where.
type AscStmt struct {
	Cols       []string
	Index      string
	Where      string
	Slice      []int
	Scols      []string
	Boundaries map[string]string
}

// DelStmt holds metadata for a DELETE statement targeting a single row.
type DelStmt struct {
	Cols  []string
	Index string
	Where string
	Slice []int
	Scols []string
}

// InsStmt holds metadata for mapping SELECT columns to INSERT columns.
type InsStmt struct {
	Cols  []string
	Slice []int
}

// CmpWhere holds a generated multi-column comparison WHERE clause with
// its corresponding column slice metadata.
type CmpWhere struct {
	Where string
	Slice []int
	Scols []string
}

// GenerateAscStmt generates metadata for ascending index traversal.
// cols is the initial SELECT column list; nil means use all table columns.
// ascFirst limits to only the first index column.
// nIndexCols limits to the first N index columns (0 means use all).
// ascOnly uses strict '>' instead of '>=' for the default where clause.
func GenerateAscStmt(tbl tableparser.TableInfo, index string, cols []string, ascFirst bool, nIndexCols int, ascOnly bool) (AscStmt, error) {
	if !tbl.KeyExists(index) {
		return AscStmt{}, fmt.Errorf("Index '%s' does not exist in table", index)
	}
    debug.Printvar("Will ascend index: ", index)

	ascCols := tbl.KeyCols(index)

	if ascFirst {
        debug.Print("Ascending only first column")
		ascCols = ascCols[0:1]
	} else if nIndexCols > 0 && nIndexCols < len(ascCols) {
        debug.Print("Ascending with only the first " + strconv.Itoa(nIndexCols) + " columns")
		ascCols = ascCols[0:nIndexCols]
	}
    debug.PrintArray("Ascending with columns: ",ascCols,", ")

	// Copy cols to avoid mutating caller's slice; default to all table columns.
	var workCols []string
	if len(cols) == 0 {
		workCols = tbl.GetCols()
	} else {
		workCols = append([]string{}, cols...)
	}

	// Build position map.
	colPosn := make(map[string]int, len(workCols))
	for i, col := range workCols {
		colPosn[col] = i
	}

	// Ensure all index columns appear in the SELECT list.
	var ascSlice []int
	for _, col := range ascCols {
		if _, exists := colPosn[col]; !exists {
			colPosn[col] = len(workCols)
			workCols = append(workCols, col)
		}
		ascSlice = append(ascSlice, colPosn[col])
	}
    debug.PrintArrayInt("Will ascend, in ordinal position: ",ascSlice,", ")

	result := AscStmt{
		Cols:       workCols,
		Index:      index,
		Boundaries: make(map[string]string),
		Slice:      []int{},
		Scols:      []string{},
	}

	if len(ascSlice) == 0 {
		return result, nil
	}

	// Build nullable and type maps for columns referenced in the WHERE clause.
	isNullable := make(map[string]bool, len(workCols))
	typeFor := make(map[string]string, len(workCols))
	for _, col := range workCols {
		isNullable[col] = tbl.ColNullable(col)
		typeFor[col] = tbl.ColType(col)
	}

	var lastCmpWhere CmpWhere
	for _, cmpType := range []string{"<", "<=", ">=", ">"} {
		cw, err := GenerateCmpWhere(cmpType, ascSlice, workCols, isNullable, typeFor)
		if err != nil {
			return AscStmt{}, err
		}
		result.Boundaries[cmpType] = cw.Where
		lastCmpWhere = cw
	}

	defaultCmp := ">="
	if ascOnly {
		defaultCmp = ">"
	}
	result.Where = result.Boundaries[defaultCmp]
	// Slice/Scols come from the last iteration (">").
	result.Slice = lastCmpWhere.Slice
	result.Scols = lastCmpWhere.Scols

	return result, nil
}

// GenerateCmpWhere generates a multi-column comparison WHERE clause.
// compareType is one of '>', '>=', '<', '<='.
// slice contains ordinal positions into cols for each index column.
// isNullable and typeFor may be nil.
func GenerateCmpWhere(compareType string, slice []int, cols []string, isNullable map[string]bool, typeFor map[string]string) (CmpWhere, error) {
	if isNullable == nil {
		isNullable = map[string]bool{}
	}
	if typeFor == nil {
		typeFor = map[string]string{}
	}

	// cmp is the strict form: ">=" → ">", "<=" → "<", ">" → ">", "<" → "<".
	cmp := strings.ReplaceAll(compareType, "=", "")
	hasEq := strings.Contains(compareType, "=")
	hasGt := strings.Contains(compareType, ">")

	var rSlice []int
	var rScols []string
	var clauses []string

	for i := range slice {
		var clause []string

		// Equality conditions for all preceding index columns.
		for j := 0; j < i; j++ {
			ord := slice[j]
			col := cols[ord]
			quo := quoter.Backtick([]string{col})
			val := placeholderFor(col, typeFor)
			if isNullable[col] {
				clause = append(clause, fmt.Sprintf("((%s IS NULL AND %s IS NULL) OR (%s = %s))", val, quo, quo, val))
				rSlice = append(rSlice, ord, ord)
				rScols = append(rScols, col, col)
			} else {
				clause = append(clause, fmt.Sprintf("%s = %s", quo, val))
				rSlice = append(rSlice, ord)
				rScols = append(rScols, col)
			}
		}

		// Comparison condition for the current index column.
		ord := slice[i]
		col := cols[ord]
		quo := quoter.Backtick([]string{col})
		val := placeholderFor(col, typeFor)
		end := i == len(slice)-1

		if isNullable[col] {
			switch {
			case hasEq && end:
				// e.g. ">=" or "<=" at the last column: (? IS NULL OR col >= ?)
				clause = append(clause, fmt.Sprintf("(%s IS NULL OR %s %s %s)", val, quo, compareType, val))
			case hasGt:
				// ">" or ">=" for non-last nullable: ((? IS NULL AND col IS NOT NULL) OR (col > ?))
				clause = append(clause, fmt.Sprintf("((%s IS NULL AND %s IS NOT NULL) OR (%s %s %s))", val, quo, quo, cmp, val))
			default:
				// "<" or "<=" for any position nullable: pushed directly (not into clause),
				// so it appears as a separate OR branch without the preceding equalities.
				clauses = append(clauses, fmt.Sprintf("((%s IS NOT NULL AND %s IS NULL) OR (%s %s %s))", val, quo, quo, cmp, val))
			}
			rSlice = append(rSlice, ord, ord)
			rScols = append(rScols, col, col)
		} else {
			if hasEq && end {
				clause = append(clause, fmt.Sprintf("%s %s %s", quo, compareType, val))
			} else {
				clause = append(clause, fmt.Sprintf("%s %s %s", quo, cmp, val))
			}
			rSlice = append(rSlice, ord)
			rScols = append(rScols, col)
		}

		if len(clause) > 0 {
			clauses = append(clauses, "("+strings.Join(clause, " AND ")+")")
		}
	}

	where := "(" + strings.Join(clauses, " OR ") + ")"
	return CmpWhere{Where: where, Slice: rSlice, Scols: rScols}, nil
}

// GenerateDelStmt generates metadata for a DELETE statement targeting a single row.
// cols is the initial SELECT column list (may be nil/empty).
// index is the preferred index name (empty string means find the best index).
func GenerateDelStmt(tbl tableparser.TableInfo, cols []string, index string) (DelStmt, error) {
	bestIndex := tbl.Findbestindex(index)

	var delCols []string
	if tbl.KeyIsUnique(bestIndex) {
		delCols = tbl.KeyCols(bestIndex)
	} else {
		delCols = tbl.GetCols()
	}

    debug.PrintArray("Columns needed for DELETE: ",delCols,", ")

	workCols := append([]string{}, cols...)

	colPosn := make(map[string]int, len(workCols))
	for i, col := range workCols {
		colPosn[col] = i
	}

	var delSlice []int
	for _, col := range delCols {
		if _, exists := colPosn[col]; !exists {
			colPosn[col] = len(workCols)
			workCols = append(workCols, col)
		}
		delSlice = append(delSlice, colPosn[col])
	}
    debug.PrintArrayInt("Ordinals needed for DELETE: ",delSlice,", ")

	result := DelStmt{
		Cols:  workCols,
		Index: bestIndex,
		Slice: []int{},
		Scols: []string{},
	}

	var clauses []string
	for _, ord := range delSlice {
		col := workCols[ord]
		quo := quoter.Backtick([]string{col})
		if tbl.ColNullable(col) {
			clauses = append(clauses, fmt.Sprintf("((? IS NULL AND %s IS NULL) OR (%s = ?))", quo, quo))
			result.Slice = append(result.Slice, ord, ord)
			result.Scols = append(result.Scols, col, col)
		} else {
			clauses = append(clauses, fmt.Sprintf("%s = ?", quo))
			result.Slice = append(result.Slice, ord)
			result.Scols = append(result.Scols, col)
		}
	}

	result.Where = "(" + strings.Join(clauses, " AND ") + ")"

	return result, nil
}

// GenerateInsStmt maps SELECT columns to INSERT columns.
// Returns the intersection of selCols and insTbl's columns, preserving selCols order.
func GenerateInsStmt(insTbl tableparser.TableInfo, selCols []string) (InsStmt, error) {
	if len(selCols) == 0 {
		return InsStmt{}, fmt.Errorf("no SELECT columns specified")
	}

	result := InsStmt{
		Cols:  []string{},
		Slice: []int{},
	}
	for i, col := range selCols {
		if insTbl.ColExists(col) {
			result.Cols = append(result.Cols, col)
			result.Slice = append(result.Slice, i)
		}
	}
	return result, nil
}

// placeholderFor returns "CAST(? AS UNSIGNED)" for enum columns, "?" otherwise.
func placeholderFor(col string, typeFor map[string]string) string {
	if typeFor[col] == "enum" {
		return "CAST(? AS UNSIGNED)"
	}
	return "?"
}
