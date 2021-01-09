import XCTest
@testable import SQLiteNano

final class SQLiteNanoTests: XCTestCase {
    
    func testUTF() {
        let database = SQLiteNano("TestUTF")
        guard database.deleteAllRows() == true else {
            fatalError("Couldn't delete all rows")
        }
        
        let list = [
            Movie(title: "3🟨🟦🟥 : Red", year: 1994),
            Movie(title: "Boyhood👦", year: 2014),
            Movie(title: "Citizen Kane", year: 1941),
            Movie(title: "The 📷Godfather", year: 1972),
            Movie(title: "Casablanca🎥", year: 1943),
        ].shuffled()
        guard database.storeMovies(list) >= 0 else {
            fatalError("Could not store movies")
        }
        
        let rowsCount = database.countRows()
        XCTAssertEqual(list.count, rowsCount)
        
        let movie = database.findMovieFor(year: 1994)
        print(movie)
        let sample = list.first(where: { $0.year == 1994 })!
        XCTAssertEqual(movie, [sample])
        database.close()
    }
    
    func testExample() {
        let database = SQLiteNano("Test")
        guard database.deleteAllRows() == true else {
            fatalError("Couldn't delete all rows")
        }
        
        let list = [
            Movie(title: "Three Colors: Red", year: 1994),
            Movie(title: "Boyhood", year: 2014),
            Movie(title: "Citizen Kane", year: 1941),
            Movie(title: "The Godfather", year: 1972),
            Movie(title: "Casablanca", year: 1943),
            Movie(title: "Three Colors: Red", year: 1994),
            Movie(title: "Boyhood", year: 2014),
            Movie(title: "Citizen Kane", year: 1941),
            Movie(title: "The Godfather", year: 1972),
            Movie(title: "Casablanca", year: 1943),
            Movie(title: "Three Colors: Red", year: 1994),
            Movie(title: "Boyhood", year: 2014),
            Movie(title: "Citizen Kane", year: 1941),
            Movie(title: "The Godfather", year: 1972),
            Movie(title: "Casablanca", year: 1943),
            Movie(title: "Three Colors: Red", year: 1994),
            Movie(title: "Boyhood", year: 2014),
            Movie(title: "Citizen Kane", year: 1941),
            Movie(title: "The Godfather", year: 1972),
            Movie(title: "Casablanca", year: 1943),
        ].shuffled()
        guard database.storeMovies(list) >= 0 else {
            fatalError("Could not store movies")
        }
        
        
        let rowsCount = database.countRows()
        XCTAssertEqual(list.count, rowsCount)
        database.close()
    }
    
    static var allTests = [
        ("testExample", testExample),
        ("testUTF", testUTF),
    ]
}
