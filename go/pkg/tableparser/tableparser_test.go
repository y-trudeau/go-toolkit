package tableparser

import (
	"testing"
)

// simpleTable is a representative CREATE TABLE with PK, unique key, composite key.
const simpleTable = "CREATE TABLE `test` (\n" +
	"  `id` int unsigned NOT NULL AUTO_INCREMENT,\n" +
	"  `a` char(40) NOT NULL,\n" +
	"  `b` char(40) NOT NULL,\n" +
	"  `c` char(40) NOT NULL,\n" +
	"  `score` decimal(10,2) DEFAULT NULL,\n" +
	"  PRIMARY KEY (`id`),\n" +
	"  UNIQUE KEY `a_unique` (`a`),\n" +
	"  KEY `bc_idx` (`b`,`c`)\n" +
	") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\n"

// fkTable has a foreign key constraint.
const fkTable = "CREATE TABLE `child` (\n" +
	"  `id` int NOT NULL,\n" +
	"  `parent_id` int NOT NULL,\n" +
	"  PRIMARY KEY (`id`),\n" +
	"  KEY `parent_idx` (`parent_id`),\n" +
	"  CONSTRAINT `child_ibfk_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON DELETE CASCADE\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\n"

// fullTextTable has FULLTEXT and SPATIAL keys.
const fullTextTable = "CREATE TABLE `articles` (\n" +
	"  `id` int NOT NULL AUTO_INCREMENT,\n" +
	"  `body` text NOT NULL,\n" +
	"  PRIMARY KEY (`id`),\n" +
	"  FULLTEXT KEY `body_ft` (`body`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\n"

// noPKTable has no primary key.
const noPKTable = "CREATE TABLE `nopk` (\n" +
	"  `a` int NOT NULL,\n" +
	"  `b` int NOT NULL,\n" +
	"  KEY `ab_idx` (`a`,`b`)\n" +
	") ENGINE=MyISAM DEFAULT CHARSET=latin1\n"

// prefixTable has an index with a column prefix.
const prefixTable = "CREATE TABLE `docs` (\n" +
	"  `id` int NOT NULL,\n" +
	"  `content` varchar(1000) NOT NULL,\n" +
	"  PRIMARY KEY (`id`),\n" +
	"  KEY `content_pfx` (`content`(100))\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\n"

func TestGetengine(t *testing.T) {
	{
		// InnoDB extracted from simpleTable
		eng, err := Getengine(simpleTable)
		if err != nil {
			t.Errorf("Getengine returned unexpected error: %v", err)
		}
		if eng != "InnoDB" {
			t.Errorf("Getengine expected 'InnoDB', got '%v'", eng)
		}
	}
	{
		// MyISAM extracted from noPKTable
		eng, err := Getengine(noPKTable)
		if err != nil {
			t.Errorf("Getengine returned unexpected error: %v", err)
		}
		if eng != "MyISAM" {
			t.Errorf("Getengine expected 'MyISAM', got '%v'", eng)
		}
	}
	{
		// Error when no ENGINE line present
		_, err := Getengine("CREATE TABLE `t` (`id` int) CHARSET=utf8mb4")
		if err == nil {
			t.Errorf("Getengine expected error for missing ENGINE, got nil")
		}
	}
}

func TestGetcharset(t *testing.T) {
	{
		// utf8mb4 extracted from simpleTable
		cs, err := Getcharset(simpleTable)
		if err != nil {
			t.Errorf("Getcharset returned unexpected error: %v", err)
		}
		if cs != "utf8mb4" {
			t.Errorf("Getcharset expected 'utf8mb4', got '%v'", cs)
		}
	}
	{
		// latin1 extracted from noPKTable
		cs, err := Getcharset(noPKTable)
		if err != nil {
			t.Errorf("Getcharset returned unexpected error: %v", err)
		}
		if cs != "latin1" {
			t.Errorf("Getcharset expected 'latin1', got '%v'", cs)
		}
	}
	{
		// Error when no DEFAULT CHARSET line present
		_, err := Getcharset(") ENGINE=InnoDB\n")
		if err == nil {
			t.Errorf("Getcharset expected error for missing CHARSET, got nil")
		}
	}
}

func TestParseIndexColumns(t *testing.T) {
	{
		// Single column
		m := parseIndexColumns("`id`")
		if len(m) != 1 {
			t.Errorf("parseIndexColumns: expected 1 entry, got %d", len(m))
		}
		if _, ok := m["id"]; !ok {
			t.Errorf("parseIndexColumns: expected key 'id' in map, got %v", m)
		}
		if m["id"].prefix != 0 {
			t.Errorf("parseIndexColumns: expected prefix 0 for 'id', got %d", m["id"].prefix)
		}
	}
	{
		// Multi-column
		m := parseIndexColumns("`b`,`c`")
		if len(m) != 2 {
			t.Errorf("parseIndexColumns: expected 2 entries, got %d", len(m))
		}
		if _, ok := m["b"]; !ok {
			t.Errorf("parseIndexColumns: expected key 'b' in map")
		}
		if _, ok := m["c"]; !ok {
			t.Errorf("parseIndexColumns: expected key 'c' in map")
		}
	}
	{
		// Column with prefix
		m := parseIndexColumns("`content`(100)")
		if len(m) != 1 {
			t.Errorf("parseIndexColumns: expected 1 entry, got %d", len(m))
		}
		if m["content"].prefix != 100 {
			t.Errorf("parseIndexColumns: expected prefix 100, got %d", m["content"].prefix)
		}
	}
}

func TestGetKeys(t *testing.T) {
	{
		// simpleTable: PRIMARY, a_unique, bc_idx
		km := GetKeys(simpleTable)
		if _, ok := km["PRIMARY"]; !ok {
			t.Errorf("GetKeys: expected PRIMARY key in result")
		}
		if !km["PRIMARY"].primary {
			t.Errorf("GetKeys: PRIMARY.primary should be true")
		}
		if !km["PRIMARY"].unique {
			t.Errorf("GetKeys: PRIMARY.unique should be true")
		}
		if km["PRIMARY"].keyType != "BTREE" {
			t.Errorf("GetKeys: PRIMARY.keyType expected 'BTREE', got '%v'", km["PRIMARY"].keyType)
		}

		if _, ok := km["a_unique"]; !ok {
			t.Errorf("GetKeys: expected 'a_unique' key in result")
		}
		if !km["a_unique"].unique {
			t.Errorf("GetKeys: a_unique.unique should be true")
		}

		if _, ok := km["bc_idx"]; !ok {
			t.Errorf("GetKeys: expected 'bc_idx' key in result")
		}
		if len(km["bc_idx"].cols) != 2 {
			t.Errorf("GetKeys: bc_idx should have 2 cols, got %d", len(km["bc_idx"].cols))
		}
	}
	{
		// fullTextTable: FULLTEXT key should have keyType "TEXT"
		km := GetKeys(fullTextTable)
		if _, ok := km["body_ft"]; !ok {
			t.Errorf("GetKeys: expected 'body_ft' key in result")
		}
		if km["body_ft"].keyType != "TEXT" {
			t.Errorf("GetKeys: body_ft.keyType expected 'TEXT', got '%v'", km["body_ft"].keyType)
		}
	}
	{
		// noPKTable: no PRIMARY key — must not panic
		km := GetKeys(noPKTable)
		if _, ok := km["PRIMARY"]; ok {
			t.Errorf("GetKeys: noPKTable should have no PRIMARY key")
		}
		if _, ok := km["ab_idx"]; !ok {
			t.Errorf("GetKeys: expected 'ab_idx' key in result")
		}
	}
	{
		// prefixTable: index column should carry the prefix
		km := GetKeys(prefixTable)
		if _, ok := km["content_pfx"]; !ok {
			t.Errorf("GetKeys: expected 'content_pfx' key in result")
		}
		if km["content_pfx"].cols["content"].prefix != 100 {
			t.Errorf("GetKeys: content_pfx column prefix expected 100, got %d", km["content_pfx"].cols["content"].prefix)
		}
	}
}

func TestGetFks(t *testing.T) {
	{
		// fkTable has one FK
		fm := GetFks(fkTable)
		if len(fm) != 1 {
			t.Errorf("GetFks: expected 1 FK, got %d", len(fm))
		}
		fk, ok := fm["`child_ibfk_1`"]
		if !ok {
			t.Errorf("GetFks: expected key '`child_ibfk_1`' in map, got %v", fm)
		}
		if fk.parenttb != "`parent`" {
			t.Errorf("GetFks: expected parenttb '`parent`', got '%v'", fk.parenttb)
		}
	}
	{
		// simpleTable has no FKs
		fm := GetFks(simpleTable)
		if len(fm) != 0 {
			t.Errorf("GetFks: expected 0 FKs for simpleTable, got %d", len(fm))
		}
	}
}

func TestSortindexes(t *testing.T) {
	{
		// simpleTable: PRIMARY should sort first
		ti, err := Parse(simpleTable)
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		sorted := ti.Sortindexes()
		if len(sorted) == 0 {
			t.Fatalf("Sortindexes returned empty slice")
		}
		if sorted[0].name != "PRIMARY" {
			t.Errorf("Sortindexes: expected PRIMARY first, got '%v'", sorted[0].name)
		}
	}
	{
		// UNIQUE key should sort before non-unique when no PRIMARY exists
		ti := TableInfo{
			keys: map[string]KeyInfo{
				"plain": {name: "plain", keyType: "BTREE", primary: false, unique: false},
				"uq":    {name: "uq", keyType: "BTREE", primary: false, unique: true},
			},
		}
		sorted := ti.Sortindexes()
		if len(sorted) < 2 {
			t.Fatalf("Sortindexes: expected 2 results, got %d", len(sorted))
		}
		if sorted[0].name != "uq" {
			t.Errorf("Sortindexes: expected unique key first, got '%v'", sorted[0].name)
		}
	}
	{
		// More cols wins over fewer cols (both non-unique, no PRIMARY)
		ti := TableInfo{
			keys: map[string]KeyInfo{
				"one": {name: "one", keyType: "BTREE", cols: map[string]KeyColInfo{"a": {}}},
				"two": {name: "two", keyType: "BTREE", cols: map[string]KeyColInfo{"a": {}, "b": {}}},
			},
		}
		sorted := ti.Sortindexes()
		if len(sorted) < 2 {
			t.Fatalf("Sortindexes: expected 2 results, got %d", len(sorted))
		}
		if sorted[0].name != "two" {
			t.Errorf("Sortindexes: expected 2-col index first, got '%v'", sorted[0].name)
		}
	}
}

func TestFindbestindex(t *testing.T) {
	{
		// Named index that exists
		ti, err := Parse(simpleTable)
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		best := ti.Findbestindex("a_unique")
		if best != "a_unique" {
			t.Errorf("Findbestindex: expected 'a_unique', got '%v'", best)
		}
	}
	{
		// Empty idx — returns best from Sortindexes (PRIMARY)
		ti, err := Parse(simpleTable)
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		best := ti.Findbestindex("")
		if best != "PRIMARY" {
			t.Errorf("Findbestindex: expected 'PRIMARY' as best, got '%v'", best)
		}
	}
	// Note: calling Findbestindex with a non-existent index calls os.Exit(1),
	// which cannot be tested without a subprocess; that path is not covered here.
}

func TestParse(t *testing.T) {
	{
		// Valid DDL: correct name, engine, charset, columns, keys
		ti, err := Parse(simpleTable)
		if err != nil {
			t.Fatalf("Parse returned unexpected error: %v", err)
		}
		if ti.name != "test" {
			t.Errorf("Parse: expected table name 'test', got '%v'", ti.name)
		}
		if ti.engine != "InnoDB" {
			t.Errorf("Parse: expected engine 'InnoDB', got '%v'", ti.engine)
		}
		if ti.charset != "utf8mb4" {
			t.Errorf("Parse: expected charset 'utf8mb4', got '%v'", ti.charset)
		}
		// Columns: id, a, b, c, score
		if len(ti.cols) != 5 {
			t.Errorf("Parse: expected 5 columns, got %d", len(ti.cols))
		}
		if _, ok := ti.cols["id"]; !ok {
			t.Errorf("Parse: expected column 'id' in cols")
		}
		if !ti.cols["id"].autoinc {
			t.Errorf("Parse: column 'id' should have autoinc=true")
		}
		if ti.cols["id"].nullable {
			t.Errorf("Parse: column 'id' should have nullable=false")
		}
		if !ti.cols["id"].numeric {
			t.Errorf("Parse: column 'id' should have numeric=true")
		}
		if ti.cols["a"].nullable {
			t.Errorf("Parse: column 'a' should have nullable=false")
		}
		if !ti.cols["score"].nullable {
			t.Errorf("Parse: column 'score' should have nullable=true")
		}
		if !ti.cols["score"].numeric {
			t.Errorf("Parse: column 'score' should have numeric=true (decimal)")
		}
		// Keys
		if _, ok := ti.keys["PRIMARY"]; !ok {
			t.Errorf("Parse: expected PRIMARY key")
		}
		if _, ok := ti.keys["a_unique"]; !ok {
			t.Errorf("Parse: expected 'a_unique' key")
		}
	}
	{
		// Empty DDL returns error
		_, err := Parse("")
		if err == nil {
			t.Errorf("Parse: expected error for empty DDL, got nil")
		}
	}
	{
		// DDL without backtick quoting returns error
		_, err := Parse("CREATE TABLE test (id int) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
		if err == nil {
			t.Errorf("Parse: expected error for unquoted DDL, got nil")
		}
	}
}
