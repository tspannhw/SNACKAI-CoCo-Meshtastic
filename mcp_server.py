#!/usr/bin/env python3
"""
Snowflake MCP Server for Meshtastic Cortex Agent
Uses REST API to query the Cortex Agent
"""
import json
import asyncio
import logging
import os
import requests
from typing import Sequence
from datetime import datetime

try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp.types import Tool, TextContent, Resource, Prompt, PromptMessage, PromptArgument
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False
    print("MCP not installed. Install with: pip install mcp")

import snowflake.connector
from snowflake.connector import DictCursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("meshtastic-mcp-server")

SNOWFLAKE_CONN = os.getenv("SNOWFLAKE_CONNECTION_NAME", "tspann1")
AGENT_FQN = "DEMO.DEMO.MESHTASTIC_AGENT"

def get_connection():
    return snowflake.connector.connect(connection_name=SNOWFLAKE_CONN)

def run_sql(sql: str) -> list:
    """Execute SQL and return results"""
    conn = get_connection()
    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute(sql)
        results = cursor.fetchall()
        return [dict(row) for row in results]
    except Exception as e:
        logger.error(f"SQL error: {e}")
        return [{"error": str(e)}]
    finally:
        cursor.close()
        conn.close()

def query_cortex_analyst(question: str) -> dict:
    """Query Cortex Analyst directly via the semantic view"""
    conn = get_connection()
    try:
        cursor = conn.cursor(DictCursor)
        safe_q = question.replace("'", "''")[:2000]
        
        sql = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large2',
            'You are an expert at converting natural language to SQL for IoT mesh network data.
            
Table: DEMO.DEMO.MESHTASTIC_DATA
Columns: from_id (device ID), packet_type (position/telemetry/text), latitude, longitude, altitude, 
battery_level, voltage, temperature, relative_humidity, rx_snr, rx_rssi, text, ingested_at

Convert this question to SQL: {safe_q}

Return ONLY the SQL query, no explanation.'
        ) as sql_query
        """
        cursor.execute(sql)
        result = cursor.fetchone()
        
        if result and result['SQL_QUERY']:
            generated_sql = result['SQL_QUERY'].strip()
            if generated_sql.startswith('```'):
                generated_sql = generated_sql.split('```')[1]
                if generated_sql.startswith('sql'):
                    generated_sql = generated_sql[3:]
            generated_sql = generated_sql.strip()
            
            if generated_sql.upper().startswith('SELECT'):
                cursor.execute(generated_sql)
                data = cursor.fetchall()
                return {
                    "question": question,
                    "sql": generated_sql,
                    "results": [dict(row) for row in data]
                }
        
        return {"error": "Could not generate SQL"}
    except Exception as e:
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()

if MCP_AVAILABLE:
    app = Server("meshtastic-mcp-server")

    @app.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="ask_mesh_analyst",
                description="Ask natural language questions about the Meshtastic mesh network. Converts questions to SQL and returns data about GPS positions, battery levels, signal quality, temperature, humidity, and network statistics.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "Natural language question about mesh network data"
                        }
                    },
                    "required": ["question"]
                }
            ),
            Tool(
                name="get_active_nodes",
                description="Get all active mesh network nodes with latest status",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "hours": {"type": "integer", "description": "Lookback hours", "default": 24}
                    }
                }
            ),
            Tool(
                name="get_node_details",
                description="Get detailed telemetry for a specific node",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "node_id": {"type": "string", "description": "Node ID (e.g., !b9d44b14)"},
                        "limit": {"type": "integer", "default": 10}
                    },
                    "required": ["node_id"]
                }
            ),
            Tool(
                name="get_network_stats",
                description="Get overall network statistics",
                inputSchema={
                    "type": "object", 
                    "properties": {
                        "hours": {"type": "integer", "default": 24}
                    }
                }
            ),
            Tool(
                name="get_gps_positions",
                description="Get latest GPS positions for all nodes",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "hours": {"type": "integer", "default": 24}
                    }
                }
            ),
            Tool(
                name="get_low_battery",
                description="Get nodes with low battery",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "threshold": {"type": "integer", "default": 20}
                    }
                }
            ),
            Tool(
                name="get_signal_quality",
                description="Analyze signal quality across network",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "hours": {"type": "integer", "default": 24}
                    }
                }
            ),
            Tool(
                name="run_sql",
                description="Run custom SELECT query on mesh data",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string", "description": "SELECT SQL query"}
                    },
                    "required": ["sql"]
                }
            )
        ]

    @app.call_tool()
    async def call_tool(name: str, arguments: dict) -> Sequence[TextContent]:
        
        if name == "ask_mesh_analyst":
            result = query_cortex_analyst(arguments.get("question", ""))
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
        
        elif name == "get_active_nodes":
            hours = arguments.get("hours", 24)
            result = run_sql(f"SELECT * FROM TABLE(DEMO.DEMO.MESH_GET_ACTIVE_NODES({hours}))")
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
        
        elif name == "get_node_details":
            node_id = arguments.get("node_id", "")
            limit = arguments.get("limit", 10)
            result = run_sql(f"""
                SELECT ingested_at, packet_type, battery_level, voltage, temperature,
                       relative_humidity, rx_snr, rx_rssi, latitude, longitude, altitude
                FROM DEMO.DEMO.MESHTASTIC_DATA
                WHERE from_id = '{node_id}'
                ORDER BY ingested_at DESC LIMIT {limit}
            """)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
        
        elif name == "get_network_stats":
            hours = arguments.get("hours", 24)
            result = run_sql(f"""
                SELECT COUNT(DISTINCT from_id) as unique_nodes,
                       COUNT(*) as total_packets,
                       AVG(battery_level) as avg_battery,
                       AVG(rx_snr) as avg_snr,
                       AVG(temperature) as avg_temp
                FROM DEMO.DEMO.MESHTASTIC_DATA
                WHERE ingested_at >= DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
            """)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
        
        elif name == "get_gps_positions":
            hours = arguments.get("hours", 24)
            result = run_sql(f"""
                SELECT from_id, latitude, longitude, altitude, ground_speed, sats_in_view, ingested_at
                FROM DEMO.DEMO.MESHTASTIC_DATA
                WHERE ingested_at >= DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
                  AND latitude IS NOT NULL AND longitude IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (PARTITION BY from_id ORDER BY ingested_at DESC) = 1
            """)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
        
        elif name == "get_low_battery":
            threshold = arguments.get("threshold", 20)
            result = run_sql(f"""
                SELECT from_id, battery_level, voltage, ingested_at
                FROM DEMO.DEMO.MESHTASTIC_DATA
                WHERE battery_level IS NOT NULL AND battery_level <= {threshold}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY from_id ORDER BY ingested_at DESC) = 1
                ORDER BY battery_level
            """)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
        
        elif name == "get_signal_quality":
            hours = arguments.get("hours", 24)
            result = run_sql(f"""
                SELECT from_id, 
                       AVG(rx_snr) as avg_snr, MIN(rx_snr) as min_snr, MAX(rx_snr) as max_snr,
                       AVG(rx_rssi) as avg_rssi, COUNT(*) as samples,
                       CASE WHEN AVG(rx_snr) >= 10 THEN 'Excellent'
                            WHEN AVG(rx_snr) >= 5 THEN 'Good'
                            WHEN AVG(rx_snr) >= 0 THEN 'Fair'
                            ELSE 'Poor' END as quality
                FROM DEMO.DEMO.MESHTASTIC_DATA
                WHERE ingested_at >= DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
                  AND rx_snr IS NOT NULL
                GROUP BY from_id ORDER BY avg_snr DESC
            """)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
        
        elif name == "run_sql":
            sql = arguments.get("sql", "")
            if not sql.strip().upper().startswith("SELECT"):
                return [TextContent(type="text", text="Error: Only SELECT queries allowed")]
            result = run_sql(sql)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
        
        return [TextContent(type="text", text=f"Unknown tool: {name}")]

    @app.list_resources()
    async def list_resources() -> list[Resource]:
        return [
            Resource(uri="mesh://status", name="Network Status", mimeType="application/json"),
            Resource(uri="mesh://nodes", name="Active Nodes", mimeType="application/json"),
            Resource(uri="mesh://positions", name="GPS Positions", mimeType="application/json")
        ]

    @app.read_resource()
    async def read_resource(uri: str) -> str:
        if uri == "mesh://status":
            return json.dumps(run_sql("""
                SELECT COUNT(DISTINCT from_id) as nodes, COUNT(*) as packets,
                       AVG(battery_level) as avg_bat, AVG(rx_snr) as avg_snr
                FROM DEMO.DEMO.MESHTASTIC_DATA
                WHERE ingested_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
            """), default=str)
        elif uri == "mesh://nodes":
            return json.dumps(run_sql("SELECT * FROM TABLE(DEMO.DEMO.MESH_GET_ACTIVE_NODES(24))"), default=str)
        elif uri == "mesh://positions":
            return json.dumps(run_sql("""
                SELECT from_id, latitude, longitude, altitude, ingested_at
                FROM DEMO.DEMO.MESHTASTIC_DATA
                WHERE latitude IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (PARTITION BY from_id ORDER BY ingested_at DESC) = 1
            """), default=str)
        return "{}"

    @app.list_prompts()
    async def list_prompts() -> list[Prompt]:
        return [
            Prompt(name="health_check", description="Network health analysis"),
            Prompt(name="node_diagnostic", description="Diagnose specific node",
                   arguments=[PromptArgument(name="node_id", required=True)])
        ]

    @app.get_prompt()
    async def get_prompt(name: str, arguments: dict = None) -> list[PromptMessage]:
        if name == "health_check":
            return [PromptMessage(role="user", content=TextContent(type="text", 
                text="Analyze mesh network health: active nodes, battery levels, signal quality, coverage gaps."))]
        elif name == "node_diagnostic":
            node = arguments.get("node_id", "") if arguments else ""
            return [PromptMessage(role="user", content=TextContent(type="text",
                text=f"Diagnose node {node}: battery, signal, position, recent activity, issues."))]
        return []

    async def main():
        logger.info(f"Starting Meshtastic MCP Server")
        logger.info(f"Agent: {AGENT_FQN}")
        async with stdio_server() as (read, write):
            await app.run(read, write, app.create_initialization_options())

if __name__ == "__main__":
    if MCP_AVAILABLE:
        asyncio.run(main())
    else:
        print("Testing SQL connection...")
        result = run_sql("SELECT * FROM TABLE(DEMO.DEMO.MESH_GET_ACTIVE_NODES(24)) LIMIT 5")
        print(json.dumps(result, indent=2, default=str))
