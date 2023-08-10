An SDK for exporting mysql data into sql files.

```go
package main

import (
	"mysqldump"
	"os"
	"strings"
)

func main() {

	// dump sql data to target.sql
	file, _ := os.Open("./target.sql")

	_ = mysqldump.Dump("your database dsn", // Required fields
		/* Unnecessary option */
		mysqldump.WithData(),                    // Export table data, not export by default
		mysqldump.WithDBs("your database name"), // If you want to export all dbs, replace it with .WithAllDatabases()
		/*
			Multiple tables are separated by commas, eg: "t1", "t2", "t3",
			If you want to export all tables, replace it with .WithAllTables()
		*/
		mysqldump.WithTables("your table name"),
		mysqldump.WithDumpTable(),                 // Export table DDL
		mysqldump.WithDropTable(),                 // Drop table after dumped
		mysqldump.WithWriter(file),                // Export destination, output to the console by default
		mysqldump.WithWhere("your sql condition"), // Where condition in SQL, eg: "id > 0 and id < 100 and score > 80"
		mysqldump.WithoutPrimaryID(true),          // Export data without primary key ID
	)

	// source sql to mysql
	_ = mysqldump.Source("your database dsn",
		strings.NewReader("insert into `user` (`id`, `name`, `score`) values (10, 'andrew', 81);"), // Insert dml, support batch insert
		mysqldump.WithMergeInsert(1000), // The number of batch inserts
	)
}

```