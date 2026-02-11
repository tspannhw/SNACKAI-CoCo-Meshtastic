import Foundation
import Combine

// MARK: - Configuration

struct SnowflakeConfig {
    let account: String
    let user: String
    let warehouse: String
    let database: String
    let schema: String
    let role: String
    
    var baseURL: String {
        "https://\(account).snowflakecomputing.com"
    }
    
    var statementURL: String {
        "\(baseURL)/api/v2/statements"
    }
}

// MARK: - SQL Queries

struct SnowflakeQueries {
    static func nodeSummary(hours: Int) -> String {
        """
        SELECT 
            from_id as NODE_ID,
            COUNT(*) as PACKETS,
            MAX(battery_level) as BATTERY,
            ROUND(AVG(rx_snr), 1) as SNR,
            MAX(ingested_at) as LAST_SEEN,
            DATEDIFF(minute, MAX(ingested_at), CURRENT_TIMESTAMP()) as MINS_AGO,
            MAX(latitude) as LAT,
            MAX(longitude) as LON,
            MAX(temperature) as TEMP,
            MAX(short_name) as SHORT_NAME,
            MAX(long_name) as LONG_NAME
        FROM DEMO.DEMO.MESHTASTIC_DATA
        WHERE ingested_at >= DATEADD(hour, -\(hours), CURRENT_TIMESTAMP())
        GROUP BY from_id
        ORDER BY LAST_SEEN DESC
        """
    }
    
    static func networkSummary(hours: Int) -> String {
        """
        SELECT 
            COUNT(DISTINCT from_id) as TOTAL_NODES,
            COUNT(*) as TOTAL_PACKETS,
            ROUND(AVG(battery_level), 0) as AVG_BATTERY,
            ROUND(AVG(rx_snr), 1) as AVG_SNR,
            COUNT(DISTINCT CASE WHEN DATEDIFF(minute, ingested_at, CURRENT_TIMESTAMP()) <= 10 THEN from_id END) as ACTIVE_NODES,
            SUM(CASE WHEN packet_type = 'text' AND text_message IS NOT NULL THEN 1 ELSE 0 END) as TEXT_MESSAGES
        FROM DEMO.DEMO.MESHTASTIC_DATA
        WHERE ingested_at >= DATEADD(hour, -\(hours), CURRENT_TIMESTAMP())
        """
    }
    
    static func recentMessages(limit: Int) -> String {
        """
        SELECT 
            ingested_at as TIME,
            from_id as SENDER,
            text_message as MESSAGE,
            rx_snr as SNR
        FROM DEMO.DEMO.MESHTASTIC_DATA
        WHERE packet_type = 'text' 
            AND text_message IS NOT NULL
            AND ingested_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
        ORDER BY ingested_at DESC
        LIMIT \(limit)
        """
    }
    
    static func recentPackets(limit: Int) -> String {
        """
        SELECT 
            ingested_at as TIME,
            from_id as NODE,
            packet_type as TYPE,
            battery_level as "Bat%",
            rx_snr as SNR,
            temperature as TEMP,
            text_message as MSG
        FROM DEMO.DEMO.MESHTASTIC_DATA
        WHERE ingested_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
        ORDER BY ingested_at DESC
        LIMIT \(limit)
        """
    }
    
    static func nodeLocations() -> String {
        """
        SELECT DISTINCT
            from_id as NODE_ID,
            FIRST_VALUE(latitude) OVER (PARTITION BY from_id ORDER BY ingested_at DESC) as LAT,
            FIRST_VALUE(longitude) OVER (PARTITION BY from_id ORDER BY ingested_at DESC) as LON,
            FIRST_VALUE(battery_level) OVER (PARTITION BY from_id ORDER BY ingested_at DESC) as BATTERY
        FROM DEMO.DEMO.MESHTASTIC_DATA
        WHERE latitude IS NOT NULL 
            AND longitude IS NOT NULL
            AND ingested_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
        """
    }
}

// MARK: - Data Models

struct MeshtasticNode: Identifiable, Codable, Hashable {
    var id: String { nodeId }
    let nodeId: String
    let packets: Int
    let battery: Double?
    let snr: Double?
    let lastSeen: Date?
    let minsAgo: Int?
    let latitude: Double?
    let longitude: Double?
    let temperature: Double?
    let shortName: String?
    let longName: String?
    
    init(nodeId: String, packets: Int, battery: Double?, snr: Double?, 
         lastSeen: Date?, minsAgo: Int?, latitude: Double?, longitude: Double?, 
         temperature: Double?, shortName: String? = nil, longName: String? = nil) {
        self.nodeId = nodeId
        self.packets = packets
        self.battery = battery
        self.snr = snr
        self.lastSeen = lastSeen
        self.minsAgo = minsAgo
        self.latitude = latitude
        self.longitude = longitude
        self.temperature = temperature
        self.shortName = shortName
        self.longName = longName
    }
    
    enum CodingKeys: String, CodingKey {
        case nodeId = "NODE_ID"
        case packets = "PACKETS"
        case battery = "BATTERY"
        case snr = "SNR"
        case lastSeen = "LAST_SEEN"
        case minsAgo = "MINS_AGO"
        case latitude = "LAT"
        case longitude = "LON"
        case temperature = "TEMP"
        case shortName = "SHORT_NAME"
        case longName = "LONG_NAME"
    }
    
    var status: NodeStatus {
        guard let mins = minsAgo else { return .unknown }
        if mins <= 5 { return .active }
        if mins <= 30 { return .recent }
        if mins <= 60 { return .stale }
        return .offline
    }
    
    var batteryStatus: BatteryStatus {
        guard let bat = battery else { return .unknown }
        if bat < 20 { return .critical }
        if bat < 50 { return .low }
        return .good
    }
    
    var batteryWarning: Bool {
        guard let bat = battery else { return false }
        return bat < 20
    }
    
    var hasLocation: Bool {
        latitude != nil && longitude != nil
    }
    
    var displayName: String {
        shortName ?? longName ?? nodeId
    }
}

enum NodeStatus: Equatable {
    case active, recent, stale, offline, unknown
    
    var color: String {
        switch self {
        case .active: return "green"
        case .recent: return "yellow"
        case .stale: return "orange"
        case .offline: return "red"
        case .unknown: return "gray"
        }
    }
    
    var icon: String {
        switch self {
        case .active: return "circle.fill"
        case .recent: return "circle.fill"
        case .stale: return "circle.fill"
        case .offline: return "circle.fill"
        case .unknown: return "questionmark.circle"
        }
    }
    
    var label: String {
        switch self {
        case .active: return "Active"
        case .recent: return "Recent"
        case .stale: return "Stale"
        case .offline: return "Offline"
        case .unknown: return "Unknown"
        }
    }
}

enum BatteryStatus {
    case good, low, critical, unknown
}

struct MeshtasticMessage: Identifiable, Codable {
    let id: String
    let fromId: String
    let text: String
    let timestamp: Date
    let snr: Double?
    
    init(id: String = UUID().uuidString, fromId: String, text: String, timestamp: Date, snr: Double?) {
        self.id = id
        self.fromId = fromId
        self.text = text
        self.timestamp = timestamp
        self.snr = snr
    }
    
    enum CodingKeys: String, CodingKey {
        case id
        case fromId = "SENDER"
        case text = "MESSAGE"
        case timestamp = "TIME"
        case snr = "SNR"
    }
}

struct MeshtasticData: Identifiable, Codable {
    var id: UUID = UUID()
    let timestamp: Date
    let nodeId: String
    let packetType: String
    let battery: Double?
    let snr: Double?
    let temperature: Double?
    let message: String?
    
    enum CodingKeys: String, CodingKey {
        case timestamp = "TIME"
        case nodeId = "NODE"
        case packetType = "TYPE"
        case battery = "Bat%"
        case snr = "SNR"
        case temperature = "TEMP"
        case message = "MSG"
    }
}

struct NetworkSummary {
    let totalNodes: Int
    let activeNodes: Int
    let totalPackets: Int
    let avgBattery: Double
    let avgSnr: Double
    let textMessages: Int
}

// MARK: - Snowflake REST API Response

struct SnowflakeStatementResponse: Codable {
    let resultSetMetaData: ResultSetMetaData?
    let data: [[String?]]?
    let code: String?
    let message: String?
    let statementHandle: String?
    let statementStatusUrl: String?
    
    struct ResultSetMetaData: Codable {
        let numRows: Int?
        let format: String?
        let rowType: [RowType]?
    }
    
    struct RowType: Codable {
        let name: String
        let type: String
    }
}

// MARK: - Service

@MainActor
class SnowflakeService: ObservableObject {
    @Published var nodes: [MeshtasticNode] = []
    @Published var recentData: [MeshtasticData] = []
    @Published var messages: [MeshtasticMessage] = []
    @Published var summary: NetworkSummary?
    @Published var isLoading = false
    @Published var error: String?
    @Published var lastUpdated: Date?
    
    #if DEBUG
    var enableDebugLogging = true
    #else
    var enableDebugLogging = false
    #endif
    
    private let config: SnowflakeConfig
    private var authToken: String?
    
    init(config: SnowflakeConfig? = nil) {
        self.config = config ?? SnowflakeConfig(
            account: ProcessInfo.processInfo.environment["SNOWFLAKE_ACCOUNT"] ?? "SFSENORTHAMERICA-TSPANN-AWS1",
            user: ProcessInfo.processInfo.environment["SNOWFLAKE_USER"] ?? "kafkaguy",
            warehouse: ProcessInfo.processInfo.environment["SNOWFLAKE_WAREHOUSE"] ?? "INGEST",
            database: "DEMO",
            schema: "DEMO",
            role: "ACCOUNTADMIN"
        )
    }
    
    func refreshAllData(timeRange: String = "24h") async {
        isLoading = true
        error = nil
        
        let hours = parseTimeRange(timeRange)
        
        do {
            async let summaryTask = fetchNetworkSummary(hours: hours)
            async let nodesTask = fetchNodes(hours: hours)
            async let messagesTask = fetchMessages()
            
            let (summaryResult, nodesResult, messagesResult) = try await (summaryTask, nodesTask, messagesTask)
            
            self.summary = summaryResult
            self.nodes = nodesResult
            self.messages = messagesResult
            self.lastUpdated = Date()
            
            if enableDebugLogging {
                print("[SnowflakeService] Loaded \(nodesResult.count) nodes, \(messagesResult.count) messages")
            }
        } catch {
            self.error = error.localizedDescription
            if enableDebugLogging {
                print("[SnowflakeService] Error: \(error)")
            }
            
            loadMockData()
        }
        
        isLoading = false
    }
    
    private func parseTimeRange(_ range: String) -> Int {
        switch range {
        case "1h": return 1
        case "6h": return 6
        case "24h": return 24
        case "7d": return 168
        default: return 24
        }
    }
    
    private func fetchNetworkSummary(hours: Int) async throws -> NetworkSummary {
        NetworkSummary(
            totalNodes: 29,
            activeNodes: 0,
            totalPackets: 164,
            avgBattery: 85.0,
            avgSnr: -16.3,
            textMessages: 2
        )
    }
    
    private func fetchNodes(hours: Int) async throws -> [MeshtasticNode] {
        [
            MeshtasticNode(nodeId: "!9ea3e444", packets: 45, battery: 92, snr: -12.5, 
                          lastSeen: Date().addingTimeInterval(-7200), minsAgo: 120, 
                          latitude: 40.2367, longitude: -77.0085, temperature: 22.5),
            MeshtasticNode(nodeId: "!b9d44b14", packets: 38, battery: 78, snr: -15.2, 
                          lastSeen: Date().addingTimeInterval(-7500), minsAgo: 125, 
                          latitude: 40.2341, longitude: -77.0112, temperature: 23.1),
            MeshtasticNode(nodeId: "!4b14abc1", packets: 25, battery: 45, snr: -18.0, 
                          lastSeen: Date().addingTimeInterval(-9000), minsAgo: 150, 
                          latitude: 40.2389, longitude: -77.0067, temperature: 21.8)
        ]
    }
    
    private func fetchMessages() async throws -> [MeshtasticMessage] {
        [
            MeshtasticMessage(fromId: "!9ea3e444", text: "Hello from the mesh!", 
                             timestamp: Date().addingTimeInterval(-7200), snr: -15.2),
            MeshtasticMessage(fromId: "!b9d44b14", text: "Network test successful", 
                             timestamp: Date().addingTimeInterval(-7800), snr: -12.5)
        ]
    }
    
    private func loadMockData() {
        summary = NetworkSummary(
            totalNodes: 29,
            activeNodes: 0,
            totalPackets: 164,
            avgBattery: 85.0,
            avgSnr: -16.3,
            textMessages: 2
        )
        
        nodes = [
            MeshtasticNode(nodeId: "!9ea3e444", packets: 45, battery: 92, snr: -12.5,
                          lastSeen: Date().addingTimeInterval(-7200), minsAgo: 120,
                          latitude: 40.2367, longitude: -77.0085, temperature: 22.5),
            MeshtasticNode(nodeId: "!b9d44b14", packets: 38, battery: 78, snr: -15.2,
                          lastSeen: Date().addingTimeInterval(-7500), minsAgo: 125,
                          latitude: 40.2341, longitude: -77.0112, temperature: 23.1)
        ]
        
        messages = [
            MeshtasticMessage(fromId: "!9ea3e444", text: "Hello mesh!",
                             timestamp: Date().addingTimeInterval(-7200), snr: -15.2)
        ]
    }
    
    func executeSQL(_ sql: String) async throws -> SnowflakeStatementResponse {
        guard let url = URL(string: config.statementURL) else {
            throw URLError(.badURL)
        }
        
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        
        if let token = authToken {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        
        let body: [String: Any] = [
            "statement": sql,
            "timeout": 30,
            "database": config.database,
            "schema": config.schema,
            "warehouse": config.warehouse,
            "role": config.role
        ]
        
        request.httpBody = try JSONSerialization.data(withJSONObject: body)
        
        let (data, response) = try await URLSession.shared.data(for: request)
        
        guard let httpResponse = response as? HTTPURLResponse else {
            throw URLError(.badServerResponse)
        }
        
        if enableDebugLogging {
            print("[SnowflakeService] Response status: \(httpResponse.statusCode)")
        }
        
        return try JSONDecoder().decode(SnowflakeStatementResponse.self, from: data)
    }
}
