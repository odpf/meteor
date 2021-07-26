package mssql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/odpf/meteor/core"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"

	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"github.com/odpf/meteor/utils"
)

var defaultDBList = []string{
	"master",
	"msdb",
	"model",
	"tempdb",
}

type Config struct {
	UserID   string `mapstructure:"user_id" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
}

type Extractor struct {
	logger plugins.Logger
}

func New() *Extractor {
	return &Extractor{}
}

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return extractor.InvalidConfigError{}
	}

	db, err := sql.Open("mssql", fmt.Sprintf("sqlserver://%s:%s@%s/", config.UserID, config.Password, config.Host))
	if err != nil {
		return
	}
	defer db.Close()
	result, err := e.getDatabases(db)
	if err != nil {
		return err
	}

	out <- result
	return
}

func (e *Extractor) getDatabases(db *sql.DB) (result []meta.Table, err error) {
	res, err := db.Query("SELECT name FROM sys.databases;")
	if err != nil {
		return
	}
	for res.Next() {
		var database string
		res.Scan(&database)
		if checkNotDefaultDatabase(database) {
			result, err = e.getTablesInfo(db, database, result)
			if err != nil {
				return
			}
		}
	}
	return
}

func (e *Extractor) getTablesInfo(db *sql.DB, dbName string, result []meta.Table) (_ []meta.Table, err error) {
	sqlStr := `SELECT TABLE_NAME FROM %s.INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_TYPE = 'BASE TABLE';`
	_, err = db.Exec(fmt.Sprintf("USE %s;", dbName))
	if err != nil {
		return
	}
	rows, err := db.Query(fmt.Sprintf(sqlStr, dbName))
	if err != nil {
		return
	}
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return
		}
		columns, err1 := e.getColumns(db, dbName, tableName)
		if err1 != nil {
			return
		}
		result = append(result, meta.Table{
			Urn:  fmt.Sprintf("%s.%s", dbName, tableName),
			Name: tableName,
			Schema: &facets.Columns{
				Columns: columns,
			},
		})
	}
	return result, err
}

func (e *Extractor) getColumns(db *sql.DB, dbName string, tableName string) (result []*facets.Column, err error) {
	sqlStr := `SELECT COLUMN_NAME,DATA_TYPE,
				IS_NULLABLE,coalesce(CHARACTER_MAXIMUM_LENGTH,0)
				FROM %s.information_schema.columns
				WHERE TABLE_NAME = '%s' 
				ORDER BY COLUMN_NAME ASC`
	rows, err := db.Query(fmt.Sprintf(sqlStr, dbName, tableName))
	if err != nil {
		return
	}
	for rows.Next() {
		var fieldName, dataType, isNullableString string
		var length int
		err = rows.Scan(&fieldName, &dataType, &isNullableString, &length)
		if err != nil {
			return
		}
		result = append(result, &facets.Column{
			Name:       fieldName,
			DataType:   dataType,
			IsNullable: e.isNullable(isNullableString),
			Length:     int64(length),
		})
	}
	return result, nil
}

func (e *Extractor) isNullable(value string) bool {
	return value == "YES"
}

func checkNotDefaultDatabase(database string) bool {
	for i := 0; i < len(defaultDBList); i++ {
		if database == defaultDBList[i] {
			return false
		}
	}
	return true
}

func init() {
	if err := extractor.Catalog.Register("mssql", func() core.Extractor {
		return &Extractor{
			logger: plugins.Log,
		}
	}); err != nil {
		panic(err)
	}
}
