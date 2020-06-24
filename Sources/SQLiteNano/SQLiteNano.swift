//
//  SQLiteNano.swift
//  SQLiteNano
//
//  Created by Hannes Sverrisson on 29/05/2020.
//  Copyright © 2020 Hannes Sverrisson. All rights reserved.
//

import Combine
import SQLite3
import Foundation
import os.log

public struct Movie: Codable, CustomStringConvertible, Identifiable, Equatable {
    var uuid: UUID
    var title: String
    var year: Int
    
    public var id: UUID {
        return uuid
    }
    
    public var description: String {
        "(\(title)|\(year)|\(uuid.uuidString))"
    }
    
    public init(title: String, year: Int, uuidString: String? = nil) {
        self.title = title
        self.year = year
        if let uuidString = uuidString,
            let uuid = UUID(uuidString: uuidString) {
            self.uuid = uuid
        } else {
            self.uuid = UUID()
        }
    }
}

public class SQLiteNano: ObservableObject {
    @Published var movies: [Movie] = []
    
    var database: OpaquePointer?
    var storeRowStmt: OpaquePointer?
    var deleteRowsStmt: OpaquePointer?
    var retrieveRowStmt: OpaquePointer?
    var findMovieStmt: OpaquePointer?
    var countStmt: OpaquePointer?
    
    /// Total rows in the table
    /// - Returns: Int of numbers of rows
    public func countRows() -> Int {
        guard database != nil else {
            os_log(.error, "DB pointer is nil")
            return 0
        }
        
        // Prepare (compile) the statement
        if (countStmt == nil) {
            // Store a movie in db
            let zSql = "SELECT COUNT(*) FROM Movies;"
            let nByte = Int32(zSql.count)
            
            if sqlite3_prepare_v2(database, zSql, nByte, &countStmt, nil) == SQLITE_OK {
                os_log(.info, "Compiled count row data")
            } else {
                os_log(.error, "Could not prepare count")
                return 0
            }
        }
        // Run the statement
        var counts: [Int32] = []
        var success = SQLITE_ROW
        while success == SQLITE_ROW {
            success = sqlite3_step(countStmt)
            let count = sqlite3_column_int(countStmt, 0)
            print("Count: \(count)")
            counts.append(count)
        }
        if success != SQLITE_DONE {
            os_log(.error, "Could not count rows")
            let errorMessage = String(cString: sqlite3_errmsg(database))
            print("Error: \(errorMessage)")
        }
        sqlite3_reset(countStmt)
        return Int(counts.first!)
    }
    
    /// Delete all rows from the table
    /// - Returns: Bool of true if deleted, otherwise false
    public func deleteAllRows() -> Bool {
        guard database != nil else {
            os_log(.error, "DB pointer is nil")
            return false
        }
        
        // Prepare (compile) the statement
        if (deleteRowsStmt == nil) {
            // Store a movie in db
            let zSql = "DELETE FROM Movies;"
            let nByte = Int32(zSql.count)
            
            if sqlite3_prepare_v2(database, zSql, nByte, &deleteRowsStmt, nil) == SQLITE_OK {
                os_log(.info, "Compiled delete row data")
            } else {
                os_log(.error, "Could not prepare delete")
                return false
            }
        }
        
        // Run the statement
        let success = sqlite3_step(deleteRowsStmt)
        if success != SQLITE_DONE {
            os_log(.error, "Could not delete rows")
            return false
        }
        sqlite3_reset(deleteRowsStmt)
        return true
    }
    
    /// Store Movies in the Movies table
    /// - Parameter movies: [Movie] movie array to insert to db
    /// - Returns: the number of movies inserted to the db
    public func storeMovies(_ movies: [Movie]) -> Int {
        var counter = 0
        guard database != nil else {
            os_log(.error, "DB pointer is nil")
            return counter
        }
        guard movies.count > 0 else {
            os_log(.info, "No movies to insert")
            return counter
        }
        
        // Prepare (compile) the statement
        if (storeRowStmt == nil) {
            // Store a movie in db
            let zSql = "INSERT INTO Movies (uuid, title, year) VALUES (?, ?, ?);"
            let nByte = Int32(zSql.count)
            
            if sqlite3_prepare_v2(database, zSql, nByte, &storeRowStmt, nil) == SQLITE_OK {
                os_log(.info, "Compiled store row data")
            } else {
                os_log(.error, "Could not prepare store for row data")
                return counter
            }
        }
        
        for movie in movies {
            storeString(storeRowStmt, column: 1, string: movie.uuid.uuidString)
            storeString(storeRowStmt, column: 2, string: movie.title)
            sqlite3_bind_int64(storeRowStmt, 3, Int64(movie.year))
            
            // Run the statement
            var success = sqlite3_step(storeRowStmt)
            while success == SQLITE_BUSY {
                sqlite3_sleep(150)
                success = sqlite3_step(storeRowStmt)
            }
            if success != SQLITE_DONE {
                os_log(.error, "Could not insert row data for %@", movie.title)
            }
            counter += 1
            sqlite3_reset(storeRowStmt)
        }
        return counter
    }
    
    func columnType(_ type: Int32) -> String {
        switch type {
            
        case SQLITE_INTEGER:
            return "Integer"
            
        case SQLITE_FLOAT:
            return "Double"
            
        case SQLITE_BLOB:
            return "BLOB"
            
        case SQLITE_NULL:
            return "Null"
            
        case SQLITE_TEXT:
            return "Text"
            
        default:
            return "Unknown"
        }
    }
    
    //    64-bit signed integer
    //    64-bit IEEE floating point number
    //    string
    //    BLOB
    //    NULL
    func retriveString(_ statement: OpaquePointer?, column: Int32) -> String {
        os_log(.info, "Retrive String")
        let columnType = sqlite3_column_type(statement, column)
        let columnName = sqlite3_column_name(statement, column)
        guard columnType == SQLITE_TEXT else {
            os_log(.error, "Incorrect type: %d %@", columnType, self.columnType(column))
            return ""
        }
        let bytes = sqlite3_column_bytes(statement, column)
        if let text = sqlite3_column_text(statement, column) {
            let string = String(cString: text)
            if string.count != bytes {
                os_log(.error, "Wrong string: %@, should length: %d, but is: %d", string, bytes, string.count)
            }
            if let columnName = columnName, let cName = String(cString: columnName, encoding: .utf8) {
                os_log(.info, "Name: %@, column: %d, value: %ld", cName, column, string)
            }
            return string
        }
        return ""
    }
    
    func retriveInt(_ statement: OpaquePointer?, column: Int32) -> Int {
        os_log(.info, "Retrive Int")
        let columnType = sqlite3_column_type(statement, column)
        let columnName = sqlite3_column_name(statement, column)
        guard columnType == SQLITE_INTEGER else {
            os_log(.error, "Incorrect type: %d %@", columnType, self.columnType(column))
            return 0
        }
        let bytes = sqlite3_column_bytes(statement, column)
        let value = sqlite3_column_int64(statement, column)
        os_log(.info, "Byte length: %d", bytes)
        let typeBytes = MemoryLayout<Int>.size
        guard bytes <= typeBytes else {
            os_log(.error, "Wrong byte length, should be equal or less than: %d, but is: %d", typeBytes, bytes)
            return 0
        }
        if let columnName = columnName, let cName = String(cString: columnName, encoding: .utf8) {
            os_log(.info, "Name: %@, column: %d, value: %ld", cName, column, value)
        }
        return Int(value)
    }
    
    func storeString(_ statement: OpaquePointer?, column: Int32, string: String) {
        sqlite3_bind_text(statement, column, string, -1, unsafeBitCast(-1, to: sqlite3_destructor_type.self))
        os_log(.info, "Stored value: %ld", string)
    }
    
    /// Retrieve all movies from the Movie table
    /// - Returns: [Movie] all the movies in the table
    public func retrieveMovies() {
        var movies: [Movie] = []
        guard database != nil else {
            os_log(.error, "DB pointer is nil")
            return
        }
        
        // Prepare (compile) the statement
        if (retrieveRowStmt == nil) {
            // Store a movie in db
            let zSql = "SELECT uuid, title, year FROM Movies;"
            let nByte = Int32(zSql.count)
            
            if sqlite3_prepare_v2(database, zSql, nByte, &retrieveRowStmt, nil) == SQLITE_OK {
                os_log(.info, "Compiled retrieve row data")
            } else {
                os_log(.error, "Could not prepare for row data")
            }
        }
        var success: Int32 = SQLITE_ROW
        while (success == SQLITE_ROW) {
            success = sqlite3_step(retrieveRowStmt)
            guard sqlite3_data_count(retrieveRowStmt) == 3 else {
                os_log(.info, "Number of data returned: %d", sqlite3_data_count(retrieveRowStmt))
                continue
            }
            let uuidString = retriveString(retrieveRowStmt, column: 0)
            let title = retriveString(retrieveRowStmt, column: 1)
            let year = retriveInt(retrieveRowStmt, column: 2)
            
            let movie = Movie(title: title, year: year, uuidString: uuidString)
            movies.append(movie)
        }
        sqlite3_reset(retrieveRowStmt)
        self.movies = movies
    }
    
    /// Find movies from the Movie table for a given year
    /// - Parameter year: Int the year for the movie
    /// - Returns: [Movie] all movies for that year
    public func findMovieFor(year: Int) -> [Movie] {
        var movies: [Movie] = []
        guard database != nil else {
            os_log(.error, "DB pointer is nil")
            return []
        }
        
        // Prepare (compile) the statement
        if (findMovieStmt == nil) {
            // Store a movie in db
            let zSql = "SELECT uuid, title, year FROM Movies WHERE year = ? ORDER BY title LIMIT 30 OFFSET 0;"
            let nByte = Int32(zSql.count)
            
            if sqlite3_prepare_v2(database, zSql, nByte, &findMovieStmt, nil) == SQLITE_OK {
                os_log(.info, "Compiled find row data")
            } else {
                os_log(.error, "Could not prepare find")
            }
        }
        var success = sqlite3_bind_int64(findMovieStmt, 1, Int64(year))
        success = SQLITE_ROW
        while (success == SQLITE_ROW) {
            success = sqlite3_step(findMovieStmt)
            guard sqlite3_data_count(findMovieStmt) == 3 else {
                os_log(.info, "Number of data returned: %d", sqlite3_data_count(findMovieStmt))
                continue
            }
            let uuidString = retriveString(findMovieStmt, column: 0)
            let title = retriveString(findMovieStmt, column: 1)
            let year = retriveInt(findMovieStmt, column: 2)
            
            let movie = Movie(title: title, year: year, uuidString: uuidString)
            movies.append(movie)
        }
        sqlite3_reset(findMovieStmt)
        return movies
    }
    
    // MARK: SQLite Setup and close down
    
    private var tableName: String
    
    public init(_ tableName: String, open: Bool = true) {
        self.tableName = tableName
        if open {
            self.open()
        }
    }
        
    public func open() {
        // Open or set up database if needed
        if let docsDirURL = try? FileManager.default.url(for: .libraryDirectory, in: .userDomainMask, appropriateFor: nil, create: true).appendingPathComponent(tableName).appendingPathExtension("db") {
            let filename = docsDirURL.absoluteString
            
            // Open file or create
            var success = sqlite3_open_v2(filename, &database, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI | SQLITE_OPEN_FULLMUTEX, nil)
            if success != SQLITE_OK {
                if let str = sqlite3_errmsg(database) {
                    let errorString = String(cString: str)
                    os_log(.error, "Could not open or create database, error: %d, %@, path: %@", errorString, filename)
                }
            }
            
            // Create table
            let sqlStatement = "CREATE TABLE IF NOT EXISTS Movies(uuid CHAR(36) NOT NULL UNIQUE, title VARCHAR(25), year INT, id INTEGER PRIMARY KEY AUTOINCREMENT);"  //NOT NULL , UNIQUE(uuid)
            var statement: UnsafeMutableRawPointer!
            var errormsg: UnsafeMutablePointer<Int8>?
            success = sqlite3_exec(database, sqlStatement, nil, &statement, &errormsg)
            guard success == SQLITE_OK else {
                os_log(.error, "Could not create table")
                if let errormsg = errormsg {
                    os_log(.error, "Error: %@", String(cString: errormsg))
                }
                sqlite3_free(errormsg)
                return
            }
            os_log(.info, "ThreadSafe: %d", sqlite3_threadsafe())
            os_log(.info, "Setup table finished, path: %@", filename)
            
            // Compile all sql requests
            
        }
    }
    
    public func close() {
        // Destroy the statements
        os_log(.info, "Deinit")
        sqlite3_finalize(storeRowStmt)
        sqlite3_finalize(retrieveRowStmt)
        sqlite3_finalize(deleteRowsStmt)
        storeRowStmt = nil;
        retrieveRowStmt = nil;
        deleteRowsStmt = nil;
        
        // Close the database properly
        sqlite3_close(database)
        os_log(.info, "Database closed")
    }
    
    deinit {
        self.close()
    }
}


