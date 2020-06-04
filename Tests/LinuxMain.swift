import XCTest

import SQLiteNanoTests

var tests = [XCTestCaseEntry]()
tests += SQLiteNanoTests.allTests()
XCTMain(tests)
