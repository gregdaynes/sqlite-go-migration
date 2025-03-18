package main

import (
	"fmt"
	"log"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

func main() {

	// Existing DB
	// CurrentDB := NewDB([]string{"file:test.db"})
	CurrentDB := NewDB([]string{"file:target.db", "./schema.sql"})
	defer CurrentDB.Close()

	// Temporary In Memory DB - Based on the schema.sql file
	CleanDB := NewDB([]string{"file:test.db?mode=memory", "./target.sql"})
	defer CleanDB.Close()

	// Apply schema changes (create tables/indicies, drop tables/indices)
	CurrentDB.ApplySchemaChanges(CleanDB)

	// Find the tables that have been altered
	// We do this before the transaction is started to ensure that the tables are in the same state
	// 1. Disable foreign keys
	CurrentDB.DisableForeignKeys()

	for tableName, table := range CurrentDB.findAlteredTables(CleanDB) {
		// 2. Start transaction
		tx, err := CurrentDB.Connection.Begin()
		if err != nil {
			log.Fatal(err)
		}

		// 3. Define create table statement with new name
		tableNameNew := tableName + "_new"

		// 4. Create new tables
		stmt := strings.Replace(table.SQL, tableName, tableNameNew, 1)
		_, err = tx.Exec(stmt)
		if err != nil {
			log.Fatal(err)
		}

		// 5. Transfer table contents to new table
		// need to get the intersection of column names of the old and new table for the insert query
		intersection := Intersect(
			CleanDB.GetColumns(tableName),
			CurrentDB.GetColumns(tableName),
		)

		cols := strings.Join(intersection[:], ", ")
		query := "INSERT INTO " + tableNameNew + " (" + cols + ") SELECT " + cols + " FROM " + tableName
		_, err = tx.Exec(query)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Inserted " + tableNameNew)

		// 6. Drop old table
		_, err = tx.Exec("DROP TABLE " + tableName)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Dropped " + tableName)

		// 7. Rename new table to old table
		_, err = tx.Exec("ALTER TABLE " + tableNameNew + " RENAME TO " + tableName)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Renamed " + tableNameNew + " to " + tableName)

		// 8. Use CREATE INDEX, CREATE TRIGGER, and CREATE VIEW to reconstruct indexes, triggers, and views associated with table X. Perhaps use the old format of the triggers, indexes, and views saved from step 3 above as a guide, making changes as appropriate for the alteration.
		idxs := CleanDB.GetSchema().GetTableIndices(tableName)
		for _, index := range idxs {
			_, err := tx.Exec(index.SQL)
			if err != nil {
				log.Fatal(err)
			}
		}

		// 9. If any views refer to table X in a way that is affected by the schema change, then drop those views using DROP VIEW and recreate them with whatever changes are necessary to accommodate the schema change using CREATE VIEW.

		// 10. If foreign key constraints were originally enabled then run PRAGMA foreign_key_check to verify that the schema change did not break any foreign key constraints.
		err = CurrentDB.Exec("PRAGMA foreign_key_check")
		if err != nil {

			log.Fatal(err)
		}

		// 11. End transaction
		err = tx.Commit()
	}

	// 12. Enable foreign keys again
	err := CurrentDB.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("🛑")
}
