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
func Select(databaseHandler *sql.DB, column string, table string,
	condition string, inputParameters ...any) ([]map[string]interface{}, error) {
	// Execute query
	query := "SELECT " + column + " FROM " + table + " " + condition
	rows, error := databaseHandler.Query(query, inputParameters...)
	if error != nil {
		return nil, fmt.Errorf(
			"failed to querying database => mysql query syntax %q => query parameters %q => %q",
			query, inputParameters, error)
	}
	// Then close rows
	defer rows.Close()
	// Get columns
	columns, error := rows.Columns()
	if error != nil {
		return nil, fmt.Errorf("failed to get columns => %q", error)
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
		error = rows.Scan(values...)
		if error != nil {
			return nil, fmt.Errorf("failed to scan rows => %q", error)
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
