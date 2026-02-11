import SwiftUI

@main
struct MeshtasticMobileApp: App {
    @StateObject private var snowflakeService = SnowflakeService()
    
    var body: some Scene {
        WindowGroup {
            ContentView()
                .environmentObject(snowflakeService)
        }
    }
}
