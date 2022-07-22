package goalMySql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/Hari-Kiri/goalApplicationSettingsLoader"
	"github.com/go-sql-driver/mysql"
)

// MySql database connection initializer
func Initialize(allowNativePassword bool) (*sql.DB, error) {
	loadDatabaseConfiguration, errorLoadDatabaseConfiguration := goalApplicationSettingsLoader.LoadDatabaseConfiguration()
	// If LoadDatabaseConfiguration() return error handle it
	if errorLoadDatabaseConfiguration != nil {
		return nil, errorLoadDatabaseConfiguration
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
	connect, errorConnect := sql.Open("mysql", configuration.FormatDSN())
	if errorConnect != nil {
		return nil, errorConnect
	}
	// Logging to console
	var mySqlVersion string
	errorSelectVersion := connect.QueryRow("SELECT VERSION()").Scan(&mySqlVersion)
	if errorSelectVersion != nil {
		return nil, errorSelectVersion
	}
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

// MySql select query for multiple rows of data.
// Please put your parameter placeholders in inputParameters to prevent SQL Injection.
func Select(databaseHandler *sql.DB, columns []string, table string,
	condition string, inputParameters ...any) ([]map[string]interface{}, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns: %q", columns)
	}
	if len(columns) == 1 {
		column := columns[0]
		// Execute query
		query := "SELECT " + column + " FROM " + table + " " + condition
		rows, errorGetRows := databaseHandler.Query(query, inputParameters...)
		if errorGetRows != nil {
			return nil, fmt.Errorf(
				"mysql select statement failed: mysql query syntax %q, query parameters %q, mysql error %s",
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
	// Extract columns parameter to syntax string
	var columnString strings.Builder
	// Get last column from columns parameter
	lastColumn := columns[len(columns)-1]
	// Delete last column from columns parameter
	columns = columns[:len(columns)-1]
	// Extract columns
	for _, column := range columns {
		columnString.WriteString(column + ", ")
	}
	// Execute query
	query := "SELECT " + columnString.String() + lastColumn + " FROM " + table + " " + condition
	rows, errorGetRows := databaseHandler.Query(query, inputParameters...)
	if errorGetRows != nil {
		return nil, fmt.Errorf(
			"mysql select statement failed: mysql query syntax %q, query parameters %q, mysql error %s",
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

// Update MySql table. On success update this method will return how many rows updated.
// Please put your parameter placeholders values in inputParameters to prevent SQL Injection.
func Update(databaseHandler *sql.DB, table string, columns []string,
	condition string, inputParameters ...any) (int, error) {
	if len(columns) == 0 {
		return 0, fmt.Errorf("no column: %q", columns)
	}
	// Single column update
	if len(columns) == 1 {
		column := columns[0] + " = ?"
		// MySql update query
		query := "UPDATE " + table + " SET " + column + " " + condition
		executeQuery, errorExecutingQuery := databaseHandler.Exec(query, inputParameters...)
		if errorExecutingQuery != nil {
			return 0, fmt.Errorf("mysql update statement failed: %v, mysql syntax: %v", errorExecutingQuery, query)
		}
		// Get rows updated
		rowsAffected, errorGetRowsAffected := executeQuery.RowsAffected()
		if errorGetRowsAffected != nil {
			return 0, fmt.Errorf("failed to get how many rows updated: %v", errorGetRowsAffected)
		}
		// Return the total of rows updated
		return int(rowsAffected), nil
	}
	// Create update value parameter placeholders
	var columnPlaceholders strings.Builder
	// Get last column from columns parameter
	lastColumn := columns[len(columns)-1] + " = ?"
	// Delete last column from columns parameter
	columns = columns[:len(columns)-1]
	for _, column := range columns {
		columnPlaceholders.WriteString(column + " = ?, ")
	}
	// MySql update query
	query := "UPDATE " + table + " SET " + columnPlaceholders.String() + lastColumn + " " + condition
	executeQuery, errorExecutingQuery := databaseHandler.Exec(query, inputParameters...)
	if errorExecutingQuery != nil {
		return 0, fmt.Errorf("mysql update statement failed: %v, mysql syntax: %v", errorExecutingQuery, query)
	}
	// Get rows updated
	rowsAffected, errorGetRowsAffected := executeQuery.RowsAffected()
	if errorGetRowsAffected != nil {
		return 0, fmt.Errorf("failed to get how many rows updated: %v", errorGetRowsAffected)
	}
	// Return the total of rows updated
	return int(rowsAffected), nil
}

// Insert into MySql table. On success update this method will return how many rows affected.
// Please put your parameter placeholders values in inputParameters to prevent SQL Injection.
func Insert(databaseHandler *sql.DB, table string, columns []string, inputParameters ...any) (int, error) {
	if len(columns) == 0 {
		return 0, fmt.Errorf("no column: %q", columns)
	}
	// Single column insert
	if len(columns) == 1 {
		column := columns[0]
		// MySql insert query
		query := "INSERT INTO " + table + " (" + column + ") VALUES (?)"
		result, errorQueryResult := databaseHandler.Exec(query, inputParameters...)
		if errorQueryResult != nil {
			return 0, fmt.Errorf("mysql insert statement failed: %v, mysql syntax: %v", errorQueryResult, query)
		}
		// Get the new album's generated ID for the client.
		rowsAffected, errorGetRowsAffected := result.RowsAffected()
		if errorGetRowsAffected != nil {
			return 0, fmt.Errorf("failed to get how many new rows: %v", errorGetRowsAffected)
		}
		// Return the new album's ID.
		return int(rowsAffected), nil
	}
	// Create value parameter placeholders
	var valuePlaceholders strings.Builder
	valuePlaceholders.WriteString("?")
	for i := 1; i < len(inputParameters); i++ {
		valuePlaceholders.WriteString(", ?")
	}
	// Extract columns parameter to syntax string
	var columnString strings.Builder
	// Get last column from columns parameter
	lastColumn := columns[len(columns)-1]
	// Delete last column from columns parameter
	columns = columns[:len(columns)-1]
	// Extract columns
	for _, column := range columns {
		columnString.WriteString(column + ", ")
	}
	// MySql insert query
	query := "INSERT INTO " + table + " (" + columnString.String() + lastColumn + ") VALUES (" +
		valuePlaceholders.String() + ")"
	result, errorQueryResult := databaseHandler.Exec(query, inputParameters...)
	if errorQueryResult != nil {
		return 0, fmt.Errorf("mysql insert statement failed: %v, mysql syntax: %v", errorQueryResult, query)
	}
	// Get the new album's generated ID for the client.
	rowsAffected, errorGetRowsAffected := result.RowsAffected()
	if errorGetRowsAffected != nil {
		return 0, fmt.Errorf("failed to get how many new rows: %v", errorGetRowsAffected)
	}
	// Return the new album's ID.
	return int(rowsAffected), nil
}

// Replace into MySql table. On success update this method will return how many rows affected.
// Please put your parameter placeholders values in inputParameters to prevent SQL Injection.
func Replace(databaseHandler *sql.DB, table string, columns []string, inputParameters ...any) (int, error) {
	if len(columns) == 0 {
		return 0, fmt.Errorf("no column: %q", columns)
	}
	// Single column insert
	if len(columns) == 1 {
		column := columns[0]
		// MySql insert query
		query := "REPLACE INTO " + table + " (" + column + ") VALUES (?)"
		result, errorQueryResult := databaseHandler.Exec(query, inputParameters...)
		if errorQueryResult != nil {
			return 0, fmt.Errorf("mysql replace statement failed: %v, mysql syntax: %v", errorQueryResult, query)
		}
		// Get the new album's generated ID for the client.
		rowsAffected, errorGetRowsAffected := result.RowsAffected()
		if errorGetRowsAffected != nil {
			return 0, fmt.Errorf("failed to get how many rows affected: %v", errorGetRowsAffected)
		}
		// Return the new album's ID.
		return int(rowsAffected), nil
	}
	// Create value parameter placeholders
	var valuePlaceholders strings.Builder
	valuePlaceholders.WriteString("?")
	for i := 1; i < len(inputParameters); i++ {
		valuePlaceholders.WriteString(", ?")
	}
	// Extract columns parameter to syntax string
	var columnString strings.Builder
	// Get last column from columns parameter
	lastColumn := columns[len(columns)-1]
	// Delete last column from columns parameter
	columns = columns[:len(columns)-1]
	// Extract columns
	for _, column := range columns {
		columnString.WriteString(column + ", ")
	}
	// MySql insert query
	query := "REPLACE INTO " + table + " (" + columnString.String() + lastColumn + ") VALUES (" +
		valuePlaceholders.String() + ")"
	result, errorQueryResult := databaseHandler.Exec(query, inputParameters...)
	if errorQueryResult != nil {
		return 0, fmt.Errorf("mysql replace statement failed: %v, mysql syntax: %v", errorQueryResult, query)
	}
	// Get the new album's generated ID for the client.
	rowsAffected, errorGetRowsAffected := result.RowsAffected()
	if errorGetRowsAffected != nil {
		return 0, fmt.Errorf("failed to get how many rows affected: %v", errorGetRowsAffected)
	}
	// Return the new album's ID.
	return int(rowsAffected), nil
}
