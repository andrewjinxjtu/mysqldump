package mysqldump

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"
)

type sourceOption struct {
	dryRun      bool
	mergeInsert int
	debug       bool
}
type SourceOption func(*sourceOption)

func WithDryRun() SourceOption {
	return func(o *sourceOption) {
		o.dryRun = true
	}
}

func WithMergeInsert(size int) SourceOption {
	return func(o *sourceOption) {
		o.mergeInsert = size
	}
}

func WithDebug() SourceOption {
	return func(o *sourceOption) {
		o.debug = true
	}
}

type dbWrapper struct {
	DB     *sql.DB
	debug  bool
	dryRun bool
}

func newDBWrapper(db *sql.DB, dryRun, debug bool) *dbWrapper {

	return &dbWrapper{
		DB:     db,
		dryRun: dryRun,
		debug:  debug,
	}
}

func (db *dbWrapper) Exec(query string, args ...interface{}) (sql.Result, error) {
	if db.debug {
		log.Printf("[debug] [query]\n%s\n", query)
	}

	if db.dryRun {
		return nil, nil
	}
	return db.DB.Exec(query, args...)
}

// Source Load the sql statement and execute it
func Source(dns string, reader io.Reader, opts ...SourceOption) error {

	start := time.Now()
	log.Printf("[info] [source] start at %s\n", start.Format("2006-01-02 15:04:05"))

	defer func() {
		end := time.Now()
		log.Printf("[info] [source] end at %s, cost %s\n", end.Format("2006-01-02 15:04:05"), end.Sub(start))
	}()

	var err error
	var db *sql.DB
	var o sourceOption
	for _, opt := range opts {
		opt(&o)
	}

	dbName, err := GetDBNameFromDNS(dns)
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}

	db, err = sql.Open("mysql", dns)
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}
	defer func() {
		_ = db.Close()
	}()

	dbWrapper := newDBWrapper(db, o.dryRun, o.debug)

	_, err = dbWrapper.Exec(fmt.Sprintf("USE %s;", dbName))
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}

	db.SetConnMaxLifetime(3600)

	r := bufio.NewReader(reader)

	_, err = dbWrapper.Exec("SET autocommit=0;")
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}

	for {
		line, err := r.ReadString(';')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("[error] %v\n", err)
			return err
		}

		dml := line

		dml, err = trim(dml)
		if err != nil {
			log.Printf("[error] [trim] %v\n", err)
			return err
		}

		// merge insert statement if mergeInsert is true
		if o.mergeInsert > 1 && strings.HasPrefix(dml, "INSERT INTO") {
			var insertSQLs []string
			insertSQLs = append(insertSQLs, dml)
			for i := 0; i < o.mergeInsert-1; i++ {
				line, err := r.ReadString(';')
				if err != nil {
					if err == io.EOF {
						break
					}
					log.Printf("[error] %v\n", err)
					return err
				}

				dml, err := trim(line)
				if err != nil {
					log.Printf("[error] [trim] %v\n", err)
					return err
				}
				if strings.HasPrefix(dml, "INSERT INTO") {
					insertSQLs = append(insertSQLs, dml)
					continue
				}

				break
			}

			dml, err = mergeInsert(insertSQLs)
			if err != nil {
				log.Printf("[error] [mergeInsert] %v\n", err)
				return err
			}
		}

		_, err = dbWrapper.Exec(dml)
		if err != nil {
			log.Printf("[error] %v\n", err)
			return err
		}
	}

	_, err = dbWrapper.Exec("COMMIT;")
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}

	_, err = dbWrapper.Exec("SET autocommit=1;")
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}

	return nil
}

// Merge insert statement
// Input:
// INSERT INTO `test` VALUES (1, 'a');
// INSERT INTO `test` VALUES (2, 'b');
// Output:
// INSERT INTO `test` VALUES (1, 'a'), (2, 'b');
func mergeInsert(insertSQLs []string) (string, error) {
	if len(insertSQLs) == 0 {
		return "", errors.New("no input provided")
	}
	builder := strings.Builder{}
	sql1 := insertSQLs[0]
	sql1 = strings.TrimSuffix(sql1, ";")
	builder.WriteString(sql1)
	for i, insertSQL := range insertSQLs[1:] {
		if i < len(insertSQLs)-1 {
			builder.WriteString(",")
		}

		valuesIdx := strings.Index(insertSQL, "VALUES")
		if valuesIdx == -1 {
			return "", errors.New("invalid SQL: missing VALUES keyword")
		}
		dml := insertSQL[valuesIdx:]
		dml = strings.TrimPrefix(dml, "VALUES")
		dml = strings.TrimSuffix(dml, ";")
		builder.WriteString(dml)

	}
	builder.WriteString(";")

	return builder.String(), nil
}

func trim(s string) (string, error) {
	s = strings.TrimLeft(s, "\n")
	s = strings.TrimSpace(s)
	return s, nil
}
