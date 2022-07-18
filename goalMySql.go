package goalMySql

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"

	"github.com/Hari-Kiri/goalApplicationSettingsLoader"
	"github.com/go-sql-driver/mysql"
)

// MySql database connection initializer
func Initialize(allowNativePassword bool) (*sql.DB, error) {
	loadDatabaseConfiguration, error := goalApplicationSettingsLoader.LoadDatabaseConfiguration()
	// If LoadDatabaseConfiguration() return error handle it
	if error != nil {
		return nil, error
	}
	configuration := mysql.Config{
		User:                 loadDatabaseConfiguration.DatabaseConfiguration.User,
		Passwd:               loadDatabaseConfiguration.DatabaseConfiguration.Password,
		Net:                  loadDatabaseConfiguration.DatabaseConfiguration.ConnectionType,
		Addr:                 loadDatabaseConfiguration.DatabaseConfiguration.Hostname,
		DBName:               loadDatabaseConfiguration.DatabaseConfiguration.DatabaseName,
		AllowNativePasswords: allowNativePassword,
	}
	// Connect to database
	connect, error := sql.Open("mysql", configuration.FormatDSN())
	if error != nil {
		return nil, error
	}
	// Logging to console
	var mySqlVersion string
	errorSelectVersion := connect.QueryRow("SELECT VERSION()").Scan(&mySqlVersion)
	if errorSelectVersion != nil {
		return nil, errorSelectVersion
	}
	log.Println("[info] goalMySql: connecting to MySql version " + mySqlVersion)
	// Return mysql connect session
	return connect, nil
}

// Ping to mysql database
func PingDatabase(databaseHandler *sql.DB) (bool, error) {
	// Test database connection
	error := databaseHandler.Ping()
	if error != nil {
		return false, error
	}
	return true, nil
}

// Select query for returning multiple rows
func Select(databaseHandler *sql.DB, selectColumn string, table string,
	condition string, inputParameters ...any) ([]map[string]interface{}, error) {
	// Execute query
	query := "SELECT " + selectColumn + " FROM " + table + " " + condition
	rows, errorGetRows := databaseHandler.Query(query, inputParameters...)
	if errorGetRows != nil {
		return nil, fmt.Errorf(
			"failed to executing query: mysql query syntax %q, query parameters %q, mysql error %s",
			query, inputParameters, errorGetRows)
	}
	// Then close rows
	defer rows.Close()
	// Get columns
	columns, errorGetColumns := rows.Columns()
	if errorGetColumns != nil {
		return nil, fmt.Errorf("failed to get columns: %s", errorGetColumns)
	}
	// Make map string interface array variable
	list := make([]map[string]interface{}, 0)
	// Iterate query result
	for rows.Next() {
		// Make temporary interface to store MySQL query result value
		values := make([]interface{}, len(columns))
		// Every values returned from MySQL query assign to a string pointer
		// and all the memory adresses store in temporary interface for further process
		for index := range columns {
			var stringPointer string
			values[index] = &stringPointer
		}
		// Scan rows from MySQL query result
		errorGetRows = rows.Scan(values...)
		if errorGetRows != nil {
			return nil, fmt.Errorf("failed to scan rows: %s", errorGetRows)
		}
		// Make map string interface variable to store temporary interface values
		mapStringInterface := make(map[string]interface{})
		// Read every pointer value from temporary interface then store all the data
		// to map string interface variable
		for index, value := range values {
			pointer := reflect.ValueOf(value)
			queryResult := pointer.Interface()
			if pointer.Kind() == reflect.Ptr {
				queryResult = pointer.Elem().Interface()
			}
			mapStringInterface[columns[index]] = queryResult
		}
		// Store all data from map string interface variable
		// to map string interface array variable
		list = append(list, mapStringInterface)
	}
	return list, nil
}

func Update(databaseHandler *sql.DB, updateTable string, column string,
	condition string, inputParameters ...any) (int64, error) {
	// MySql update query
	query := "UPDATE " + updateTable + " SET " + column + " WHERE " + condition
	executeQuery, errorExecutingQuery := databaseHandler.Exec(query, inputParameters)
	if errorExecutingQuery != nil {
		return 0, fmt.Errorf("failed to executing query: %v", errorExecutingQuery)
	}
	// Get rows updated
	rowsAffected, errorGetRowsAffected := executeQuery.RowsAffected()
	if errorGetRowsAffected != nil {
		return 0, fmt.Errorf("failed to get how many rows updated: %v", errorGetRowsAffected)
	}
	// Return the total of rows updated
	return rowsAffected, nil
}
