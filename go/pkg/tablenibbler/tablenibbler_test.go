package tablenibbler

import (
	"reflect"
	"testing"

	"github.com/y-trudeau/go-toolkit/go/pkg/tableparser"
)

// DDL constants mirror the Perl test sample files.
const sakilaFilm = `CREATE TABLE ` + "`film`" + ` (
  ` + "`film_id`" + ` smallint(5) unsigned NOT NULL auto_increment,
  ` + "`title`" + ` varchar(255) NOT NULL,
  ` + "`description`" + ` text default NULL,
  ` + "`release_year`" + ` year(4) default NULL,
  ` + "`language_id`" + ` tinyint(3) unsigned NOT NULL,
  ` + "`original_language_id`" + ` tinyint(3) unsigned default NULL,
  ` + "`rental_duration`" + ` tinyint(3) unsigned NOT NULL default '3',
  ` + "`rental_rate`" + ` decimal(4,2) NOT NULL default '4.99',
  ` + "`length`" + ` smallint(5) unsigned default NULL,
  ` + "`replacement_cost`" + ` decimal(5,2) NOT NULL default '19.99',
  ` + "`rating`" + ` enum('G','PG','PG-13','R','NC-17') default 'G',
  ` + "`special_features`" + ` set('Trailers','Commentaries','Deleted Scenes','Behind the Scenes') default NULL,
  ` + "`last_update`" + ` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY (` + "`film_id`" + `),
  KEY ` + "`idx_title`" + ` (` + "`title`" + `),
  KEY ` + "`idx_fk_language_id`" + ` (` + "`language_id`" + `),
  KEY ` + "`idx_fk_original_language_id`" + ` (` + "`original_language_id`" + `),
  CONSTRAINT ` + "`fk_film_language`" + ` FOREIGN KEY (` + "`language_id`" + `) REFERENCES ` + "`language`" + ` (` + "`language_id`" + `) ON UPDATE CASCADE,
  CONSTRAINT ` + "`fk_film_language_original`" + ` FOREIGN KEY (` + "`original_language_id`" + `) REFERENCES ` + "`language`" + ` (` + "`language_id`" + `) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8`

const sakilaRental = `CREATE TABLE ` + "`rental`" + ` (
  ` + "`rental_id`" + ` int(11) NOT NULL auto_increment,
  ` + "`rental_date`" + ` datetime NOT NULL,
  ` + "`inventory_id`" + ` mediumint(8) unsigned NOT NULL,
  ` + "`customer_id`" + ` smallint(5) unsigned NOT NULL,
  ` + "`return_date`" + ` datetime default NULL,
  ` + "`staff_id`" + ` tinyint(3) unsigned NOT NULL,
  ` + "`last_update`" + ` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY (` + "`rental_id`" + `),
  UNIQUE KEY ` + "`rental_date`" + ` (` + "`rental_date`" + `,` + "`inventory_id`" + `,` + "`customer_id`" + `),
  KEY ` + "`idx_fk_inventory_id`" + ` (` + "`inventory_id`" + `),
  KEY ` + "`idx_fk_customer_id`" + ` (` + "`customer_id`" + `),
  KEY ` + "`idx_fk_staff_id`" + ` (` + "`staff_id`" + `),
  CONSTRAINT ` + "`fk_rental_customer`" + ` FOREIGN KEY (` + "`customer_id`" + `) REFERENCES ` + "`customer`" + ` (` + "`customer_id`" + `) ON UPDATE CASCADE,
  CONSTRAINT ` + "`fk_rental_inventory`" + ` FOREIGN KEY (` + "`inventory_id`" + `) REFERENCES ` + "`inventory`" + ` (` + "`inventory_id`" + `) ON UPDATE CASCADE,
  CONSTRAINT ` + "`fk_rental_staff`" + ` FOREIGN KEY (` + "`staff_id`" + `) REFERENCES ` + "`staff`" + ` (` + "`staff_id`" + `) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8`

// customer_id is nullable
const sakilaRentalNull = `CREATE TABLE ` + "`rental`" + ` (
  ` + "`rental_id`" + ` int(11) NOT NULL auto_increment,
  ` + "`rental_date`" + ` datetime NOT NULL,
  ` + "`inventory_id`" + ` mediumint(8) unsigned NOT NULL,
  ` + "`customer_id`" + ` smallint(5) unsigned default NULL,
  ` + "`return_date`" + ` datetime default NULL,
  ` + "`staff_id`" + ` tinyint(3) unsigned NOT NULL,
  ` + "`last_update`" + ` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY (` + "`rental_id`" + `),
  UNIQUE KEY ` + "`rental_date`" + ` (` + "`rental_date`" + `,` + "`inventory_id`" + `,` + "`customer_id`" + `),
  KEY ` + "`idx_fk_inventory_id`" + ` (` + "`inventory_id`" + `),
  KEY ` + "`idx_fk_customer_id`" + ` (` + "`customer_id`" + `),
  KEY ` + "`idx_fk_staff_id`" + ` (` + "`staff_id`" + `),
  CONSTRAINT ` + "`fk_rental_customer`" + ` FOREIGN KEY (` + "`customer_id`" + `) REFERENCES ` + "`customer`" + ` (` + "`customer_id`" + `) ON UPDATE CASCADE,
  CONSTRAINT ` + "`fk_rental_inventory`" + ` FOREIGN KEY (` + "`inventory_id`" + `) REFERENCES ` + "`inventory`" + ` (` + "`inventory_id`" + `) ON UPDATE CASCADE,
  CONSTRAINT ` + "`fk_rental_staff`" + ` FOREIGN KEY (` + "`staff_id`" + `) REFERENCES ` + "`staff`" + ` (` + "`staff_id`" + `) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8`

// inventory_id is nullable
const sakilaRentalNull2 = `CREATE TABLE ` + "`rental`" + ` (
  ` + "`rental_id`" + ` int(11) NOT NULL auto_increment,
  ` + "`rental_date`" + ` datetime NOT NULL,
  ` + "`inventory_id`" + ` mediumint(8) unsigned default NULL,
  ` + "`customer_id`" + ` smallint(5) unsigned NOT NULL,
  ` + "`return_date`" + ` datetime default NULL,
  ` + "`staff_id`" + ` tinyint(3) unsigned NOT NULL,
  ` + "`last_update`" + ` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY (` + "`rental_id`" + `),
  UNIQUE KEY ` + "`rental_date`" + ` (` + "`rental_date`" + `,` + "`inventory_id`" + `,` + "`customer_id`" + `),
  KEY ` + "`idx_fk_inventory_id`" + ` (` + "`inventory_id`" + `),
  KEY ` + "`idx_fk_customer_id`" + ` (` + "`customer_id`" + `),
  KEY ` + "`idx_fk_staff_id`" + ` (` + "`staff_id`" + `),
  CONSTRAINT ` + "`fk_rental_customer`" + ` FOREIGN KEY (` + "`customer_id`" + `) REFERENCES ` + "`customer`" + ` (` + "`customer_id`" + `) ON UPDATE CASCADE,
  CONSTRAINT ` + "`fk_rental_inventory`" + ` FOREIGN KEY (` + "`inventory_id`" + `) REFERENCES ` + "`inventory`" + ` (` + "`inventory_id`" + `) ON UPDATE CASCADE,
  CONSTRAINT ` + "`fk_rental_staff`" + ` FOREIGN KEY (` + "`staff_id`" + `) REFERENCES ` + "`staff`" + ` (` + "`staff_id`" + `) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8`

// customer_id and inventory_id swapped in table order
const sakilaRentalRemix = `CREATE TABLE ` + "`rental`" + ` (
  ` + "`rental_id`" + ` int(11) NOT NULL auto_increment,
  ` + "`rental_date`" + ` datetime NOT NULL,
  ` + "`customer_id`" + ` smallint(5) unsigned NOT NULL,
  ` + "`inventory_id`" + ` mediumint(8) unsigned NOT NULL,
  ` + "`return_date`" + ` datetime default NULL,
  ` + "`staff_id`" + ` tinyint(3) unsigned NOT NULL,
  ` + "`last_update`" + ` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY (` + "`rental_id`" + `),
  UNIQUE KEY ` + "`rental_date`" + ` (` + "`rental_date`" + `,` + "`inventory_id`" + `,` + "`customer_id`" + `),
  KEY ` + "`idx_fk_inventory_id`" + ` (` + "`inventory_id`" + `),
  KEY ` + "`idx_fk_customer_id`" + ` (` + "`customer_id`" + `),
  KEY ` + "`idx_fk_staff_id`" + ` (` + "`staff_id`" + `),
  CONSTRAINT ` + "`fk_rental_customer`" + ` FOREIGN KEY (` + "`customer_id`" + `) REFERENCES ` + "`customer`" + ` (` + "`customer_id`" + `) ON UPDATE CASCADE,
  CONSTRAINT ` + "`fk_rental_inventory`" + ` FOREIGN KEY (` + "`inventory_id`" + `) REFERENCES ` + "`inventory`" + ` (` + "`inventory_id`" + `) ON UPDATE CASCADE,
  CONSTRAINT ` + "`fk_rental_staff`" + ` FOREIGN KEY (` + "`staff_id`" + `) REFERENCES ` + "`staff`" + ` (` + "`staff_id`" + `) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8`

const issue131Sel = `CREATE TABLE ` + "`issue_131_sel`" + ` (
  ` + "`id`" + ` int(11) default NULL,
  ` + "`foo`" + ` varchar(16) default NULL,
  ` + "`name`" + ` varchar(16) default NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1`

const issue131Ins = `CREATE TABLE ` + "`issue_131_sel`" + ` (
  ` + "`name`" + ` varchar(16) default NULL,
  ` + "`id`" + ` int(11) default NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1`

func mustParse(t *testing.T, ddl string) tableparser.TableInfo {
	t.Helper()
	tbl, err := tableparser.Parse(ddl)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	return tbl
}

func TestGenerateAscStmt(t *testing.T) {
	// sakila.film: asc stmt on PRIMARY with all cols
	{
		tbl := mustParse(t, sakilaFilm)
		got, err := GenerateAscStmt(tbl, "PRIMARY", tbl.GetCols(), false, 0, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := AscStmt{
			Cols:  []string{"film_id", "title", "description", "release_year", "language_id", "original_language_id", "rental_duration", "rental_rate", "length", "replacement_cost", "rating", "special_features", "last_update"},
			Index: "PRIMARY",
			Where: "((`film_id` >= ?))",
			Slice: []int{0},
			Scols: []string{"film_id"},
			Boundaries: map[string]string{
				">=": "((`film_id` >= ?))",
				">":  "((`film_id` > ?))",
				"<=": "((`film_id` <= ?))",
				"<":  "((`film_id` < ?))",
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("asc stmt on sakila.film\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// sakila.film: defaults to all columns when cols is nil
	{
		tbl := mustParse(t, sakilaFilm)
		got, err := GenerateAscStmt(tbl, "PRIMARY", nil, false, 0, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := AscStmt{
			Cols:  []string{"film_id", "title", "description", "release_year", "language_id", "original_language_id", "rental_duration", "rental_rate", "length", "replacement_cost", "rating", "special_features", "last_update"},
			Index: "PRIMARY",
			Where: "((`film_id` >= ?))",
			Slice: []int{0},
			Scols: []string{"film_id"},
			Boundaries: map[string]string{
				">=": "((`film_id` >= ?))",
				">":  "((`film_id` > ?))",
				"<=": "((`film_id` <= ?))",
				"<":  "((`film_id` < ?))",
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("defaults to all columns\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// sakila.film: error on nonexistent index
	{
		tbl := mustParse(t, sakilaFilm)
		_, err := GenerateAscStmt(tbl, "title", tbl.GetCols(), false, 0, false)
		if err == nil {
			t.Error("expected error on nonexistent index, got nil")
		}
	}

	// sakila.film: different index idx_title
	{
		tbl := mustParse(t, sakilaFilm)
		got, err := GenerateAscStmt(tbl, "idx_title", tbl.GetCols(), false, 0, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := AscStmt{
			Cols:  []string{"film_id", "title", "description", "release_year", "language_id", "original_language_id", "rental_duration", "rental_rate", "length", "replacement_cost", "rating", "special_features", "last_update"},
			Index: "idx_title",
			Where: "((`title` >= ?))",
			Slice: []int{1},
			Scols: []string{"title"},
			Boundaries: map[string]string{
				">=": "((`title` >= ?))",
				">":  "((`title` > ?))",
				"<=": "((`title` <= ?))",
				"<":  "((`title` < ?))",
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("idx_title on sakila.film\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// sakila.film: required index columns added to SELECT list
	{
		tbl := mustParse(t, sakilaFilm)
		got, err := GenerateAscStmt(tbl, "PRIMARY", []string{"title"}, false, 0, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := AscStmt{
			Cols:  []string{"title", "film_id"},
			Index: "PRIMARY",
			Where: "((`film_id` >= ?))",
			Slice: []int{1},
			Scols: []string{"film_id"},
			Boundaries: map[string]string{
				">=": "((`film_id` >= ?))",
				">":  "((`film_id` > ?))",
				"<=": "((`film_id` <= ?))",
				"<":  "((`film_id` < ?))",
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("required columns added to SELECT list\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// sakila.rental: composite unique index rental_date
	{
		tbl := mustParse(t, sakilaRental)
		got, err := GenerateAscStmt(tbl, "rental_date", tbl.GetCols(), false, 0, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := AscStmt{
			Cols:  []string{"rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update"},
			Index: "rental_date",
			Where: "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` >= ?))",
			Slice: []int{1, 1, 2, 1, 2, 3},
			Scols: []string{"rental_date", "rental_date", "inventory_id", "rental_date", "inventory_id", "customer_id"},
			Boundaries: map[string]string{
				">=": "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` >= ?))",
				">":  "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` > ?))",
				"<=": "((`rental_date` < ?) OR (`rental_date` = ? AND `inventory_id` < ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` <= ?))",
				"<":  "((`rental_date` < ?) OR (`rental_date` = ? AND `inventory_id` < ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` < ?))",
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("rental_date index on sakila.rental\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// sakila.rental: asc_first (only first index column)
	{
		tbl := mustParse(t, sakilaRental)
		got, err := GenerateAscStmt(tbl, "rental_date", tbl.GetCols(), true, 0, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := AscStmt{
			Cols:  []string{"rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update"},
			Index: "rental_date",
			Where: "((`rental_date` >= ?))",
			Slice: []int{1},
			Scols: []string{"rental_date"},
			Boundaries: map[string]string{
				">=": "((`rental_date` >= ?))",
				">":  "((`rental_date` > ?))",
				"<=": "((`rental_date` <= ?))",
				"<":  "((`rental_date` < ?))",
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("asc_first on sakila.rental\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// sakila.rental: n_index_cols=2
	{
		tbl := mustParse(t, sakilaRental)
		got, err := GenerateAscStmt(tbl, "rental_date", tbl.GetCols(), false, 2, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := AscStmt{
			Cols:  []string{"rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update"},
			Index: "rental_date",
			Where: "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` >= ?))",
			Slice: []int{1, 1, 2},
			Scols: []string{"rental_date", "rental_date", "inventory_id"},
			Boundaries: map[string]string{
				">=": "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` >= ?))",
				">":  "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?))",
				"<=": "((`rental_date` < ?) OR (`rental_date` = ? AND `inventory_id` <= ?))",
				"<":  "((`rental_date` < ?) OR (`rental_date` = ? AND `inventory_id` < ?))",
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("n_index_cols=2 on sakila.rental\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// sakila.rental: n_index_cols=5 (> actual count, should not crash)
	{
		tbl := mustParse(t, sakilaRental)
		got, err := GenerateAscStmt(tbl, "rental_date", tbl.GetCols(), false, 5, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Same as using all 3 index columns
		want := AscStmt{
			Cols:  []string{"rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update"},
			Index: "rental_date",
			Where: "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` >= ?))",
			Slice: []int{1, 1, 2, 1, 2, 3},
			Scols: []string{"rental_date", "rental_date", "inventory_id", "rental_date", "inventory_id", "customer_id"},
			Boundaries: map[string]string{
				">=": "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` >= ?))",
				">":  "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` > ?))",
				"<=": "((`rental_date` < ?) OR (`rental_date` = ? AND `inventory_id` < ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` <= ?))",
				"<":  "((`rental_date` < ?) OR (`rental_date` = ? AND `inventory_id` < ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` < ?))",
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("n_index_cols=5 on sakila.rental\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// sakila.rental: asc_only (strict ascending, no >=)
	{
		tbl := mustParse(t, sakilaRental)
		got, err := GenerateAscStmt(tbl, "rental_date", tbl.GetCols(), false, 0, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := AscStmt{
			Cols:  []string{"rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update"},
			Index: "rental_date",
			Where: "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` > ?))",
			Slice: []int{1, 1, 2, 1, 2, 3},
			Scols: []string{"rental_date", "rental_date", "inventory_id", "rental_date", "inventory_id", "customer_id"},
			Boundaries: map[string]string{
				">=": "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` >= ?))",
				">":  "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` > ?))",
				"<=": "((`rental_date` < ?) OR (`rental_date` = ? AND `inventory_id` < ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` <= ?))",
				"<":  "((`rental_date` < ?) OR (`rental_date` = ? AND `inventory_id` < ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` < ?))",
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("asc_only on sakila.rental\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// sakila.rental.null: nullable customer_id (last index column)
	{
		tbl := mustParse(t, sakilaRentalNull)
		got, err := GenerateAscStmt(tbl, "rental_date", tbl.GetCols(), false, 0, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := AscStmt{
			Cols:  []string{"rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update"},
			Index: "rental_date",
			Where: "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND (? IS NULL OR `customer_id` >= ?)))",
			Slice: []int{1, 1, 2, 1, 2, 3, 3},
			Scols: []string{"rental_date", "rental_date", "inventory_id", "rental_date", "inventory_id", "customer_id", "customer_id"},
			Boundaries: map[string]string{
				">=": "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND (? IS NULL OR `customer_id` >= ?)))",
				">":  "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND ((? IS NULL AND `customer_id` IS NOT NULL) OR (`customer_id` > ?))))",
				"<=": "((`rental_date` < ?) OR (`rental_date` = ? AND `inventory_id` < ?) OR (`rental_date` = ? AND `inventory_id` = ? AND (? IS NULL OR `customer_id` <= ?)))",
				"<":  "((`rental_date` < ?) OR (`rental_date` = ? AND `inventory_id` < ?) OR ((? IS NOT NULL AND `customer_id` IS NULL) OR (`customer_id` < ?)) OR (`rental_date` = ? AND `inventory_id` = ?))",
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("nullable customer_id on sakila.rental\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// sakila.rental.null: nullable customer_id, asc_only
	{
		tbl := mustParse(t, sakilaRentalNull)
		got, err := GenerateAscStmt(tbl, "rental_date", tbl.GetCols(), false, 0, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := AscStmt{
			Cols:  []string{"rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update"},
			Index: "rental_date",
			Where: "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND ((? IS NULL AND `customer_id` IS NOT NULL) OR (`customer_id` > ?))))",
			Slice: []int{1, 1, 2, 1, 2, 3, 3},
			Scols: []string{"rental_date", "rental_date", "inventory_id", "rental_date", "inventory_id", "customer_id", "customer_id"},
			Boundaries: map[string]string{
				">=": "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND (? IS NULL OR `customer_id` >= ?)))",
				">":  "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND ((? IS NULL AND `customer_id` IS NOT NULL) OR (`customer_id` > ?))))",
				"<=": "((`rental_date` < ?) OR (`rental_date` = ? AND `inventory_id` < ?) OR (`rental_date` = ? AND `inventory_id` = ? AND (? IS NULL OR `customer_id` <= ?)))",
				"<":  "((`rental_date` < ?) OR (`rental_date` = ? AND `inventory_id` < ?) OR ((? IS NOT NULL AND `customer_id` IS NULL) OR (`customer_id` < ?)) OR (`rental_date` = ? AND `inventory_id` = ?))",
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("nullable customer_id asc_only on sakila.rental\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// sakila.rental.null2: nullable inventory_id (middle index column)
	{
		tbl := mustParse(t, sakilaRentalNull2)
		got, err := GenerateAscStmt(tbl, "rental_date", tbl.GetCols(), false, 0, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := AscStmt{
			Cols:  []string{"rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update"},
			Index: "rental_date",
			Where: "((`rental_date` > ?) OR (`rental_date` = ? AND ((? IS NULL AND `inventory_id` IS NOT NULL) OR (`inventory_id` > ?))) OR (`rental_date` = ? AND ((? IS NULL AND `inventory_id` IS NULL) OR (`inventory_id` = ?)) AND `customer_id` >= ?))",
			Slice: []int{1, 1, 2, 2, 1, 2, 2, 3},
			Scols: []string{"rental_date", "rental_date", "inventory_id", "inventory_id", "rental_date", "inventory_id", "inventory_id", "customer_id"},
			Boundaries: map[string]string{
				">=": "((`rental_date` > ?) OR (`rental_date` = ? AND ((? IS NULL AND `inventory_id` IS NOT NULL) OR (`inventory_id` > ?))) OR (`rental_date` = ? AND ((? IS NULL AND `inventory_id` IS NULL) OR (`inventory_id` = ?)) AND `customer_id` >= ?))",
				">":  "((`rental_date` > ?) OR (`rental_date` = ? AND ((? IS NULL AND `inventory_id` IS NOT NULL) OR (`inventory_id` > ?))) OR (`rental_date` = ? AND ((? IS NULL AND `inventory_id` IS NULL) OR (`inventory_id` = ?)) AND `customer_id` > ?))",
				"<=": "((`rental_date` < ?) OR ((? IS NOT NULL AND `inventory_id` IS NULL) OR (`inventory_id` < ?)) OR (`rental_date` = ?) OR (`rental_date` = ? AND ((? IS NULL AND `inventory_id` IS NULL) OR (`inventory_id` = ?)) AND `customer_id` <= ?))",
				"<":  "((`rental_date` < ?) OR ((? IS NOT NULL AND `inventory_id` IS NULL) OR (`inventory_id` < ?)) OR (`rental_date` = ?) OR (`rental_date` = ? AND ((? IS NULL AND `inventory_id` IS NULL) OR (`inventory_id` = ?)) AND `customer_id` < ?))",
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("nullable inventory_id on sakila.rental\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// sakila.rental.null2: nullable inventory_id, asc_only
	{
		tbl := mustParse(t, sakilaRentalNull2)
		got, err := GenerateAscStmt(tbl, "rental_date", tbl.GetCols(), false, 0, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := AscStmt{
			Cols:  []string{"rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update"},
			Index: "rental_date",
			Where: "((`rental_date` > ?) OR (`rental_date` = ? AND ((? IS NULL AND `inventory_id` IS NOT NULL) OR (`inventory_id` > ?))) OR (`rental_date` = ? AND ((? IS NULL AND `inventory_id` IS NULL) OR (`inventory_id` = ?)) AND `customer_id` > ?))",
			Slice: []int{1, 1, 2, 2, 1, 2, 2, 3},
			Scols: []string{"rental_date", "rental_date", "inventory_id", "inventory_id", "rental_date", "inventory_id", "inventory_id", "customer_id"},
			Boundaries: map[string]string{
				">=": "((`rental_date` > ?) OR (`rental_date` = ? AND ((? IS NULL AND `inventory_id` IS NOT NULL) OR (`inventory_id` > ?))) OR (`rental_date` = ? AND ((? IS NULL AND `inventory_id` IS NULL) OR (`inventory_id` = ?)) AND `customer_id` >= ?))",
				">":  "((`rental_date` > ?) OR (`rental_date` = ? AND ((? IS NULL AND `inventory_id` IS NOT NULL) OR (`inventory_id` > ?))) OR (`rental_date` = ? AND ((? IS NULL AND `inventory_id` IS NULL) OR (`inventory_id` = ?)) AND `customer_id` > ?))",
				"<=": "((`rental_date` < ?) OR ((? IS NOT NULL AND `inventory_id` IS NULL) OR (`inventory_id` < ?)) OR (`rental_date` = ?) OR (`rental_date` = ? AND ((? IS NULL AND `inventory_id` IS NULL) OR (`inventory_id` = ?)) AND `customer_id` <= ?))",
				"<":  "((`rental_date` < ?) OR ((? IS NOT NULL AND `inventory_id` IS NULL) OR (`inventory_id` < ?)) OR (`rental_date` = ?) OR (`rental_date` = ? AND ((? IS NULL AND `inventory_id` IS NULL) OR (`inventory_id` = ?)) AND `customer_id` < ?))",
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("nullable inventory_id asc_only on sakila.rental\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// sakila.rental.remix: columns in different table order
	{
		tbl := mustParse(t, sakilaRentalRemix)
		got, err := GenerateAscStmt(tbl, "rental_date", nil, false, 0, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// In remix: rental_id(0), rental_date(1), customer_id(2), inventory_id(3), ...
		// rental_date index: rental_date(1), inventory_id(3), customer_id(2)
		want := AscStmt{
			Cols:  []string{"rental_id", "rental_date", "customer_id", "inventory_id", "return_date", "staff_id", "last_update"},
			Index: "rental_date",
			Where: "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` >= ?))",
			Slice: []int{1, 1, 3, 1, 3, 2},
			Scols: []string{"rental_date", "rental_date", "inventory_id", "rental_date", "inventory_id", "customer_id"},
			Boundaries: map[string]string{
				">=": "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` >= ?))",
				">":  "((`rental_date` > ?) OR (`rental_date` = ? AND `inventory_id` > ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` > ?))",
				"<=": "((`rental_date` < ?) OR (`rental_date` = ? AND `inventory_id` < ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` <= ?))",
				"<":  "((`rental_date` < ?) OR (`rental_date` = ? AND `inventory_id` < ?) OR (`rental_date` = ? AND `inventory_id` = ? AND `customer_id` < ?))",
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("out-of-order index on sakila.rental.remix\ngot:  %+v\nwant: %+v", got, want)
		}
	}
}

func TestGenerateDelStmt(t *testing.T) {
	// sakila.film: del stmt using PRIMARY (unique)
	{
		tbl := mustParse(t, sakilaFilm)
		got, err := GenerateDelStmt(tbl, nil, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := DelStmt{
			Cols:  []string{"film_id"},
			Index: "PRIMARY",
			Where: "(`film_id` = ?)",
			Slice: []int{0},
			Scols: []string{"film_id"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("del stmt on sakila.film\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// sakila.film: del stmt with non-unique idx_title uses all columns
	{
		tbl := mustParse(t, sakilaFilm)
		got, err := GenerateDelStmt(tbl, []string{"film_id"}, "idx_title")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := DelStmt{
			Cols:  []string{"film_id", "title", "description", "release_year", "language_id", "original_language_id", "rental_duration", "rental_rate", "length", "replacement_cost", "rating", "special_features", "last_update"},
			Index: "idx_title",
			Where: "(`film_id` = ? AND `title` = ? AND ((? IS NULL AND `description` IS NULL) OR (`description` = ?)) AND ((? IS NULL AND `release_year` IS NULL) OR (`release_year` = ?)) AND `language_id` = ? AND ((? IS NULL AND `original_language_id` IS NULL) OR (`original_language_id` = ?)) AND `rental_duration` = ? AND `rental_rate` = ? AND ((? IS NULL AND `length` IS NULL) OR (`length` = ?)) AND `replacement_cost` = ? AND ((? IS NULL AND `rating` IS NULL) OR (`rating` = ?)) AND ((? IS NULL AND `special_features` IS NULL) OR (`special_features` = ?)) AND `last_update` = ?)",
			Slice: []int{0, 1, 2, 2, 3, 3, 4, 5, 5, 6, 7, 8, 8, 9, 10, 10, 11, 11, 12},
			Scols: []string{"film_id", "title", "description", "description", "release_year", "release_year", "language_id", "original_language_id", "original_language_id", "rental_duration", "rental_rate", "length", "length", "replacement_cost", "rating", "rating", "special_features", "special_features", "last_update"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("del stmt with idx_title on sakila.film\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// sakila.rental: del stmt with rental_date (unique)
	{
		tbl := mustParse(t, sakilaRental)
		got, err := GenerateDelStmt(tbl, nil, "rental_date")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := DelStmt{
			Cols:  []string{"rental_date", "inventory_id", "customer_id"},
			Index: "rental_date",
			Where: "(`rental_date` = ? AND `inventory_id` = ? AND `customer_id` = ?)",
			Slice: []int{0, 1, 2},
			Scols: []string{"rental_date", "inventory_id", "customer_id"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("del stmt on sakila.rental with rental_date index\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// sakila.rental.null: nullable customer_id del stmt
	{
		tbl := mustParse(t, sakilaRentalNull)
		got, err := GenerateDelStmt(tbl, nil, "rental_date")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := DelStmt{
			Cols:  []string{"rental_date", "inventory_id", "customer_id"},
			Index: "rental_date",
			Where: "(`rental_date` = ? AND `inventory_id` = ? AND ((? IS NULL AND `customer_id` IS NULL) OR (`customer_id` = ?)))",
			Slice: []int{0, 1, 2, 2},
			Scols: []string{"rental_date", "inventory_id", "customer_id", "customer_id"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("del stmt with nullable customer_id on sakila.rental\ngot:  %+v\nwant: %+v", got, want)
		}
	}
}

func TestGenerateInsStmt(t *testing.T) {
	// Insert from rental into film: only last_update matches
	{
		filmTbl := mustParse(t, sakilaFilm)
		rentalTbl := mustParse(t, sakilaRental)
		got, err := GenerateInsStmt(filmTbl, rentalTbl.GetCols())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := InsStmt{
			Cols:  []string{"last_update"},
			Slice: []int{6},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("insert film from rental\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// issue_131: different column order and a missing insert column
	{
		selTbl := mustParse(t, issue131Sel)
		insTbl := mustParse(t, issue131Ins)
		got, err := GenerateInsStmt(insTbl, selTbl.GetCols())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// sel: id(0), foo(1), name(2)  →  ins has id and name
		want := InsStmt{
			Cols:  []string{"id", "name"},
			Slice: []int{0, 2},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("insert with different col order and missing col\ngot:  %+v\nwant: %+v", got, want)
		}
	}
}

func TestGenerateCmpWhere(t *testing.T) {
	cols := []string{"a", "b", "c", "d"}
	slice := []int{0, 3}

	// WHERE for >=
	{
		got, err := GenerateCmpWhere(">=", slice, cols, map[string]bool{}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := CmpWhere{
			Scols: []string{"a", "a", "d"},
			Slice: []int{0, 0, 3},
			Where: "((`a` > ?) OR (`a` = ? AND `d` >= ?))",
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("WHERE for >=\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// WHERE for >
	{
		got, err := GenerateCmpWhere(">", slice, cols, map[string]bool{}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := CmpWhere{
			Scols: []string{"a", "a", "d"},
			Slice: []int{0, 0, 3},
			Where: "((`a` > ?) OR (`a` = ? AND `d` > ?))",
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("WHERE for >\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// WHERE for <=
	{
		got, err := GenerateCmpWhere("<=", slice, cols, map[string]bool{}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := CmpWhere{
			Scols: []string{"a", "a", "d"},
			Slice: []int{0, 0, 3},
			Where: "((`a` < ?) OR (`a` = ? AND `d` <= ?))",
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("WHERE for <=\ngot:  %+v\nwant: %+v", got, want)
		}
	}

	// WHERE for <
	{
		got, err := GenerateCmpWhere("<", slice, cols, map[string]bool{}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := CmpWhere{
			Scols: []string{"a", "a", "d"},
			Slice: []int{0, 0, 3},
			Where: "((`a` < ?) OR (`a` = ? AND `d` < ?))",
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("WHERE for <\ngot:  %+v\nwant: %+v", got, want)
		}
	}
}
