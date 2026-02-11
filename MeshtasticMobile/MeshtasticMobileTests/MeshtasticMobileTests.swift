import XCTest
@testable import MeshtasticMobile

final class MeshtasticMobileTests: XCTestCase {
    
    // MARK: - MeshtasticNode Tests
    
    func testNodeStatusActive() {
        let node = MeshtasticNode(
            nodeId: "!test123",
            packets: 10,
            battery: 85.0,
            snr: -5.0,
            lastSeen: Date(),
            minsAgo: 3,
            latitude: 40.0,
            longitude: -75.0,
            temperature: 22.0
        )
        
        XCTAssertEqual(node.status, .active)
        XCTAssertTrue(node.hasLocation)
    }
    
    func testNodeStatusRecent() {
        let node = MeshtasticNode(
            nodeId: "!test456",
            packets: 5,
            battery: 50.0,
            snr: -10.0,
            lastSeen: Date().addingTimeInterval(-900), // 15 mins ago
            minsAgo: 15,
            latitude: nil,
            longitude: nil,
            temperature: nil
        )
        
        XCTAssertEqual(node.status, .recent)
        XCTAssertFalse(node.hasLocation)
    }
    
    func testNodeStatusStale() {
        let node = MeshtasticNode(
            nodeId: "!test789",
            packets: 2,
            battery: 20.0,
            snr: -15.0,
            lastSeen: Date().addingTimeInterval(-2700), // 45 mins ago
            minsAgo: 45,
            latitude: 40.0,
            longitude: -75.0,
            temperature: nil
        )
        
        XCTAssertEqual(node.status, .stale)
    }
    
    func testNodeStatusOffline() {
        let node = MeshtasticNode(
            nodeId: "!testABC",
            packets: 1,
            battery: nil,
            snr: nil,
            lastSeen: Date().addingTimeInterval(-7200), // 2 hours ago
            minsAgo: 120,
            latitude: nil,
            longitude: nil,
            temperature: nil
        )
        
        XCTAssertEqual(node.status, .offline)
    }
    
    func testNodeStatusUnknown() {
        let node = MeshtasticNode(
            nodeId: "!testXYZ",
            packets: 0,
            battery: nil,
            snr: nil,
            lastSeen: nil,
            minsAgo: nil,
            latitude: nil,
            longitude: nil,
            temperature: nil
        )
        
        XCTAssertEqual(node.status, .unknown)
    }
    
    func testNodeBatteryWarning() {
        let lowBatteryNode = MeshtasticNode(
            nodeId: "!low",
            packets: 1,
            battery: 15.0,
            snr: nil,
            lastSeen: nil,
            minsAgo: 5,
            latitude: nil,
            longitude: nil,
            temperature: nil
        )
        
        let healthyNode = MeshtasticNode(
            nodeId: "!healthy",
            packets: 1,
            battery: 80.0,
            snr: nil,
            lastSeen: nil,
            minsAgo: 5,
            latitude: nil,
            longitude: nil,
            temperature: nil
        )
        
        XCTAssertTrue(lowBatteryNode.batteryWarning)
        XCTAssertFalse(healthyNode.batteryWarning)
    }
    
    // MARK: - MeshtasticMessage Tests
    
    func testMessageInit() {
        let message = MeshtasticMessage(
            id: "msg1",
            fromId: "!sender",
            text: "Hello mesh!",
            timestamp: Date(),
            snr: -8.0
        )
        
        XCTAssertEqual(message.fromId, "!sender")
        XCTAssertEqual(message.text, "Hello mesh!")
        XCTAssertEqual(message.snr, -8.0)
    }
    
    // MARK: - SnowflakeConfig Tests
    
    func testSnowflakeConfigURL() {
        let config = SnowflakeConfig(
            account: "myaccount-region",
            user: "testuser",
            warehouse: "COMPUTE_WH",
            database: "DEMO",
            schema: "PUBLIC",
            role: "ANALYST"
        )
        
        XCTAssertEqual(config.baseURL, "https://myaccount-region.snowflakecomputing.com")
        XCTAssertEqual(config.statementURL, "https://myaccount-region.snowflakecomputing.com/api/v2/statements")
    }
    
    // MARK: - SQL Query Tests
    
    func testNodeSummarySQL() {
        let sql = SnowflakeQueries.nodeSummary(hours: 24)
        
        XCTAssertTrue(sql.contains("from_id"))
        XCTAssertTrue(sql.contains("battery_level"))
        XCTAssertTrue(sql.contains("DATEADD(hour, -24"))
        XCTAssertTrue(sql.contains("GROUP BY"))
    }
    
    func testMessagesSQL() {
        let sql = SnowflakeQueries.recentMessages(limit: 50)
        
        XCTAssertTrue(sql.contains("text_message"))
        XCTAssertTrue(sql.contains("packet_type = 'text'"))
        XCTAssertTrue(sql.contains("LIMIT 50"))
    }
}

// MARK: - Test Helpers

extension MeshtasticMobileTests {
    
    func createMockNodes(count: Int) -> [MeshtasticNode] {
        (0..<count).map { i in
            MeshtasticNode(
                nodeId: "!node\(i)",
                packets: Int.random(in: 1...100),
                battery: Double.random(in: 0...100),
                snr: Double.random(in: -20...10),
                lastSeen: Date().addingTimeInterval(Double(-i * 300)),
                minsAgo: i * 5,
                latitude: 40.0 + Double.random(in: -0.1...0.1),
                longitude: -75.0 + Double.random(in: -0.1...0.1),
                temperature: Double.random(in: 15...30)
            )
        }
    }
}
