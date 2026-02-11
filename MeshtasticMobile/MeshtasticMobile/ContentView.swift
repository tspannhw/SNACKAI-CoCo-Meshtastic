import SwiftUI

struct ContentView: View {
    @EnvironmentObject var service: SnowflakeService
    @State private var selectedTimeRange = "24h"
    @State private var selectedTab = 0
    
    let timeRanges = ["1h", "6h", "24h", "7d"]
    
    var body: some View {
        NavigationView {
            ZStack {
                LinearGradient(
                    gradient: Gradient(colors: [Color(hex: "0a0a1a"), Color(hex: "1a1a2e")]),
                    startPoint: .top,
                    endPoint: .bottom
                )
                .ignoresSafeArea()
                
                VStack(spacing: 0) {
                    headerView
                    
                    summaryCards
                    
                    tabContent
                }
            }
            .navigationBarHidden(true)
        }
        .preferredColorScheme(.dark)
        .task {
            await service.refreshAllData(timeRange: selectedTimeRange)
        }
        .refreshable {
            await service.refreshAllData(timeRange: selectedTimeRange)
        }
    }
    
    var headerView: some View {
        HStack {
            VStack(alignment: .leading, spacing: 4) {
                Text("📱 Mesh Network")
                    .font(.title)
                    .fontWeight(.bold)
                    .foregroundColor(.white)
                
                if let lastUpdated = service.lastUpdated {
                    Text("Updated \(lastUpdated, style: .time)")
                        .font(.caption)
                        .foregroundColor(.gray)
                }
            }
            
            Spacer()
            
            Picker("Time", selection: $selectedTimeRange) {
                ForEach(timeRanges, id: \.self) { range in
                    Text(range).tag(range)
                }
            }
            .pickerStyle(.segmented)
            .frame(width: 150)
            .onChange(of: selectedTimeRange) { newValue in
                Task {
                    await service.refreshAllData(timeRange: newValue)
                }
            }
        }
        .padding()
    }
    
    var summaryCards: some View {
        LazyVGrid(columns: [
            GridItem(.flexible()),
            GridItem(.flexible())
        ], spacing: 12) {
            MetricCard(
                icon: "circle.fill",
                iconColor: .green,
                title: "Active",
                value: "\(service.summary?.activeNodes ?? 0)"
            )
            
            MetricCard(
                icon: "antenna.radiowaves.left.and.right",
                iconColor: .blue,
                title: "Total Nodes",
                value: "\(service.summary?.totalNodes ?? 0)"
            )
            
            MetricCard(
                icon: "battery.75",
                iconColor: .yellow,
                title: "Avg Battery",
                value: "\(Int(service.summary?.avgBattery ?? 0))%"
            )
            
            MetricCard(
                icon: "waveform",
                iconColor: .cyan,
                title: "Avg SNR",
                value: String(format: "%.1f dB", service.summary?.avgSnr ?? 0)
            )
        }
        .padding(.horizontal)
    }
    
    var tabContent: some View {
        VStack(spacing: 0) {
            HStack(spacing: 0) {
                TabButton(title: "Nodes", icon: "list.bullet", isSelected: selectedTab == 0) {
                    selectedTab = 0
                }
                TabButton(title: "Messages", icon: "message.fill", isSelected: selectedTab == 1) {
                    selectedTab = 1
                }
                TabButton(title: "Map", icon: "map.fill", isSelected: selectedTab == 2) {
                    selectedTab = 2
                }
            }
            .padding(.horizontal)
            .padding(.top, 16)
            
            TabView(selection: $selectedTab) {
                NodesListView()
                    .tag(0)
                
                MessagesView()
                    .tag(1)
                
                MapView()
                    .tag(2)
            }
            .tabViewStyle(.page(indexDisplayMode: .never))
        }
    }
}

struct MetricCard: View {
    let icon: String
    let iconColor: Color
    let title: String
    let value: String
    
    var body: some View {
        VStack(spacing: 8) {
            HStack {
                Image(systemName: icon)
                    .foregroundColor(iconColor)
                    .font(.system(size: 14))
                
                Text(title)
                    .font(.caption)
                    .foregroundColor(.gray)
                
                Spacer()
            }
            
            HStack {
                Text(value)
                    .font(.system(size: 24, weight: .bold))
                    .foregroundColor(Color(hex: "00ff88"))
                
                Spacer()
            }
        }
        .padding()
        .background(
            RoundedRectangle(cornerRadius: 12)
                .fill(
                    LinearGradient(
                        gradient: Gradient(colors: [Color(hex: "1e1e3f"), Color(hex: "2d2d5a")]),
                        startPoint: .topLeading,
                        endPoint: .bottomTrailing
                    )
                )
        )
        .overlay(
            RoundedRectangle(cornerRadius: 12)
                .stroke(Color(hex: "3d3d7a"), lineWidth: 1)
        )
    }
}

struct TabButton: View {
    let title: String
    let icon: String
    let isSelected: Bool
    let action: () -> Void
    
    var body: some View {
        Button(action: action) {
            VStack(spacing: 4) {
                Image(systemName: icon)
                    .font(.system(size: 16))
                
                Text(title)
                    .font(.caption)
            }
            .foregroundColor(isSelected ? Color(hex: "00ff88") : .gray)
            .padding(.vertical, 8)
            .padding(.horizontal, 16)
            .background(
                RoundedRectangle(cornerRadius: 8)
                    .fill(isSelected ? Color(hex: "3d3d7a") : Color.clear)
            )
        }
        .frame(maxWidth: .infinity)
    }
}

struct NodesListView: View {
    @EnvironmentObject var service: SnowflakeService
    
    var body: some View {
        ScrollView {
            LazyVStack(spacing: 12) {
                ForEach(service.nodes) { node in
                    NodeCard(node: node)
                }
            }
            .padding()
        }
    }
}

struct NodeCard: View {
    let node: MeshtasticNode
    
    var statusColor: Color {
        switch node.status {
        case .active: return .green
        case .recent: return .yellow
        case .stale: return .orange
        case .offline: return .red
        case .unknown: return .gray
        }
    }
    
    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            Circle()
                .fill(statusColor)
                .frame(width: 10, height: 10)
                .padding(.top, 6)
            
            VStack(alignment: .leading, spacing: 4) {
                HStack {
                    Text(node.nodeId)
                        .font(.headline)
                        .foregroundColor(.white)
                    
                    Spacer()
                    
                    if let mins = node.minsAgo {
                        Text("\(mins)m ago")
                            .font(.caption)
                            .foregroundColor(statusColor)
                    }
                }
                
                HStack(spacing: 16) {
                    Label("\(Int(node.battery ?? 0))%", systemImage: "battery.75")
                        .font(.caption)
                        .foregroundColor(node.batteryStatus == .critical ? .red : .gray)
                    
                    if let snr = node.snr {
                        Label(String(format: "%.1f dB", snr), systemImage: "waveform")
                            .font(.caption)
                            .foregroundColor(.gray)
                    }
                    
                    Label("\(node.packets)", systemImage: "envelope.fill")
                        .font(.caption)
                        .foregroundColor(.gray)
                    
                    if let temp = node.temperature {
                        Label(String(format: "%.1f°", temp), systemImage: "thermometer")
                            .font(.caption)
                            .foregroundColor(.gray)
                    }
                }
            }
        }
        .padding()
        .background(
            RoundedRectangle(cornerRadius: 12)
                .fill(Color(hex: "1a1a2e"))
        )
        .overlay(
            RoundedRectangle(cornerRadius: 12)
                .stroke(Color(hex: "3d3d7a").opacity(0.5), lineWidth: 1)
        )
        .overlay(
            Rectangle()
                .fill(statusColor)
                .frame(width: 4)
                .padding(.vertical, 4),
            alignment: .leading
        )
    }
}

struct MessagesView: View {
    @EnvironmentObject var service: SnowflakeService
    
    var body: some View {
        ScrollView {
            LazyVStack(spacing: 12) {
                if service.messages.isEmpty {
                    Text("No messages in selected time range")
                        .foregroundColor(.gray)
                        .padding(.top, 40)
                } else {
                    ForEach(service.messages) { message in
                        MessageBubble(message: message)
                    }
                }
            }
            .padding()
        }
    }
}

struct MessageBubble: View {
    let message: MeshtasticMessage
    
    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text(message.sender)
                    .font(.caption)
                    .fontWeight(.semibold)
                    .foregroundColor(Color(hex: "00ff88"))
                
                Spacer()
                
                Text(message.timestamp, style: .time)
                    .font(.caption2)
                    .foregroundColor(.gray)
                
                if let snr = message.snr {
                    Text(String(format: "%.1f dB", snr))
                        .font(.caption2)
                        .foregroundColor(.gray)
                }
            }
            
            Text(message.message)
                .font(.body)
                .foregroundColor(.white)
        }
        .padding()
        .background(
            RoundedRectangle(cornerRadius: 16)
                .fill(Color(hex: "2a2a4a"))
        )
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}

struct MapView: View {
    @EnvironmentObject var service: SnowflakeService
    
    var body: some View {
        VStack {
            if service.nodes.filter({ $0.latitude != nil }).isEmpty {
                Text("No GPS data available")
                    .foregroundColor(.gray)
                    .padding(.top, 40)
            } else {
                VStack(alignment: .leading, spacing: 12) {
                    Text("Node Coordinates")
                        .font(.headline)
                        .foregroundColor(.white)
                        .padding(.horizontal)
                    
                    ForEach(service.nodes.filter { $0.latitude != nil }) { node in
                        HStack {
                            Circle()
                                .fill(node.status == .active ? Color.green : Color.yellow)
                                .frame(width: 8, height: 8)
                            
                            Text(node.nodeId)
                                .font(.subheadline)
                                .foregroundColor(.white)
                            
                            Spacer()
                            
                            if let lat = node.latitude, let lon = node.longitude {
                                Text(String(format: "%.5f, %.5f", lat, lon))
                                    .font(.caption)
                                    .foregroundColor(.gray)
                                    .fontDesign(.monospaced)
                            }
                        }
                        .padding(.horizontal)
                    }
                }
                .padding(.vertical)
            }
            
            Spacer()
        }
    }
}

extension Color {
    init(hex: String) {
        let hex = hex.trimmingCharacters(in: CharacterSet.alphanumerics.inverted)
        var int: UInt64 = 0
        Scanner(string: hex).scanHexInt64(&int)
        let a, r, g, b: UInt64
        switch hex.count {
        case 3:
            (a, r, g, b) = (255, (int >> 8) * 17, (int >> 4 & 0xF) * 17, (int & 0xF) * 17)
        case 6:
            (a, r, g, b) = (255, int >> 16, int >> 8 & 0xFF, int & 0xFF)
        case 8:
            (a, r, g, b) = (int >> 24, int >> 16 & 0xFF, int >> 8 & 0xFF, int & 0xFF)
        default:
            (a, r, g, b) = (1, 1, 1, 0)
        }
        self.init(
            .sRGB,
            red: Double(r) / 255,
            green: Double(g) / 255,
            blue: Double(b) / 255,
            opacity: Double(a) / 255
        )
    }
}

#Preview {
    ContentView()
        .environmentObject(SnowflakeService())
}
