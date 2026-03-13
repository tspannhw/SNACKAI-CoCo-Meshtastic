#!/usr/bin/env python3
"""
Generate Architecture Diagram for Meshtastic Dashboard
======================================================
Creates a PNG diagram showing the system architecture using PIL.
"""

from PIL import Image, ImageDraw, ImageFont
import os

def create_architecture_diagram():
    width, height = 1600, 1200
    bg_color = (26, 26, 46)
    img = Image.new('RGB', (width, height), bg_color)
    draw = ImageDraw.Draw(img)
    
    colors = {
        'device': (74, 222, 128),
        'pipeline': (96, 165, 250),
        'storage': (244, 114, 182),
        'app': (251, 191, 36),
        'ai': (167, 139, 250),
        'user': (248, 113, 113),
        'arrow': (148, 163, 184),
        'text': (255, 255, 255),
        'subtext': (148, 163, 184),
        'dark_text': (26, 26, 46),
    }
    
    try:
        title_font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 36)
        subtitle_font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 24)
        box_font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 18)
        small_font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 14)
        label_font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 16)
    except:
        title_font = ImageFont.load_default()
        subtitle_font = title_font
        box_font = title_font
        small_font = title_font
        label_font = title_font
    
    def draw_rounded_rect(x, y, w, h, color, radius=15):
        draw.rounded_rectangle([x, y, x+w, y+h], radius=radius, fill=color, outline='white', width=2)
    
    def draw_box(x, y, w, h, color, icon, label, sublabel=None):
        draw_rounded_rect(x, y, w, h, color, radius=12)
        text_color = colors['dark_text']
        
        full_label = f"{icon} {label}"
        bbox = draw.textbbox((0, 0), full_label, font=box_font)
        text_w = bbox[2] - bbox[0]
        text_y = y + h//2 - 10 if sublabel else y + h//2
        draw.text((x + w//2 - text_w//2, text_y - 8), full_label, fill=text_color, font=box_font)
        
        if sublabel:
            bbox2 = draw.textbbox((0, 0), sublabel, font=small_font)
            sub_w = bbox2[2] - bbox2[0]
            draw.text((x + w//2 - sub_w//2, text_y + 18), sublabel, fill=text_color, font=small_font)
    
    def draw_arrow(x1, y1, x2, y2, label=None):
        draw.line([(x1, y1), (x2, y2)], fill=colors['arrow'], width=2)
        
        dx = x2 - x1
        dy = y2 - y1
        length = (dx**2 + dy**2) ** 0.5
        if length > 0:
            dx, dy = dx/length, dy/length
            arrow_size = 10
            px = x2 - arrow_size * dx
            py = y2 - arrow_size * dy
            draw.polygon([
                (x2, y2),
                (int(px - arrow_size*0.5*dy), int(py + arrow_size*0.5*dx)),
                (int(px + arrow_size*0.5*dy), int(py - arrow_size*0.5*dx))
            ], fill=colors['arrow'])
        
        if label:
            mx, my = (x1 + x2)//2, (y1 + y2)//2 - 15
            draw.text((mx, my), label, fill=colors['subtext'], font=small_font)
    
    title = "Meshtastic Mesh Network Dashboard Architecture"
    bbox = draw.textbbox((0, 0), title, font=title_font)
    title_w = bbox[2] - bbox[0]
    draw.text((width//2 - title_w//2, 30), title, fill=colors['text'], font=title_font)
    
    subtitle = "Real-time IoT Monitoring with Snowflake & Cortex AI"
    bbox2 = draw.textbbox((0, 0), subtitle, font=subtitle_font)
    sub_w = bbox2[2] - bbox2[0]
    draw.text((width//2 - sub_w//2, 75), subtitle, fill=colors['subtext'], font=subtitle_font)
    
    col1, col2, col3, col4 = 100, 450, 800, 1150
    row1, row2, row3 = 180, 330, 480
    box_w, box_h = 250, 100
    
    draw.text((col1 + 60, 130), "DATA SOURCES", fill=colors['device'], font=label_font)
    draw_box(col1, row1, box_w, box_h, colors['device'], "📡", "SenseCAP T1000-E", "LoRa GPS Tracker")
    draw_box(col1, row2, box_w, box_h, colors['device'], "📻", "Meshtastic Nodes", "Mesh Network")
    draw_box(col1, row3, box_w, box_h, colors['device'], "🔌", "BLE/Serial", "Connection")
    
    draw.text((col2 + 30, 130), "INGESTION PIPELINE", fill=colors['pipeline'], font=label_font)
    draw_box(col2, row1, box_w, box_h, colors['pipeline'], "🐍", "Python Streamer", "meshtastic_snowflake")
    draw_box(col2, row2, box_w, box_h, colors['pipeline'], "❄️", "Snowpipe v2", "REST API")
    draw_box(col2, row3, box_w, box_h, colors['pipeline'], "✅", "Validation", "Data Quality")
    
    draw.text((col3 + 30, 130), "SNOWFLAKE STORAGE", fill=colors['storage'], font=label_font)
    draw_box(col3, row1, box_w, box_h, colors['storage'], "📊", "MESHTASTIC_DATA", "Raw Packets")
    draw_box(col3, row2, box_w, box_h, colors['storage'], "⚡", "Dynamic Tables", "Live Aggregates")
    draw_box(col3, row3, box_w, box_h, colors['storage'], "📋", "Node Summary", "Device States")
    
    draw.text((col4 + 50, 130), "AI & ANALYTICS", fill=colors['ai'], font=label_font)
    draw_box(col4, row1, box_w, box_h, colors['ai'], "🤖", "Cortex Agent", "Natural Language")
    draw_box(col4, row2, box_w, box_h, colors['ai'], "🧠", "Cortex Analyst", "Text-to-SQL")
    draw_box(col4, row3, box_w, box_h, colors['ai'], "📝", "Semantic Model", "YAML Schema")
    
    draw_arrow(col1 + box_w, row1 + box_h//2, col2, row1 + box_h//2, "Packets")
    draw_arrow(col1 + box_w, row2 + box_h//2, col2, row2 + box_h//2, "Stream")
    draw_arrow(col1 + box_w//2, row1 + box_h, col1 + box_w//2, row2, "")
    draw_arrow(col1 + box_w//2, row2 + box_h, col1 + box_w//2, row3, "")
    
    draw_arrow(col2 + box_w, row1 + box_h//2, col3, row1 + box_h//2, "Insert")
    draw_arrow(col2 + box_w, row2 + box_h//2, col3, row2 + box_h//2, "REST")
    draw_arrow(col2 + box_w//2, row1 + box_h, col2 + box_w//2, row2, "")
    draw_arrow(col2 + box_w//2, row2 + box_h, col2 + box_w//2, row3, "")
    
    draw_arrow(col3 + box_w, row1 + box_h//2, col4, row1 + box_h//2, "Query")
    draw_arrow(col3 + box_w, row2 + box_h//2, col4, row2 + box_h//2, "SQL Gen")
    draw_arrow(col3 + box_w//2, row1 + box_h, col3 + box_w//2, row2, "")
    draw_arrow(col3 + box_w//2, row2 + box_h, col3 + box_w//2, row3, "")
    
    draw_arrow(col4 + box_w//2, row1 + box_h, col4 + box_w//2, row2, "")
    draw_arrow(col4 + box_w//2, row2 + box_h, col4 + box_w//2, row3, "")
    
    dash_row = 650
    draw.text((width//2 - 100, dash_row - 30), "STREAMLIT DASHBOARD", fill=colors['app'], font=label_font)
    
    dash_boxes = [
        (200, dash_row, "🗺️", "Folium Map", "Interactive"),
        (470, dash_row, "🔍", "Search", "Location Query"),
        (740, dash_row, "💬", "AI Chat", "Agent Interface"),
        (1010, dash_row, "📈", "Analytics", "Charts & Graphs"),
    ]
    
    dash_w, dash_h = 200, 80
    for x, y, icon, label, sub in dash_boxes:
        draw_box(x, y, dash_w, dash_h, colors['app'], icon, label, sub)
    
    user_x, user_y = width//2 - 100, 800
    draw_box(user_x, user_y, 200, 80, colors['user'], "👤", "User", "Browser")
    
    draw_arrow(col3 + box_w//2, row3 + box_h, width//2, dash_row, "Data")
    draw_arrow(col4 + box_w//2, row3 + box_h, 840, dash_row, "AI Response")
    
    for x, y, icon, label, sub in dash_boxes:
        draw_arrow(x + dash_w//2, y + dash_h, user_x + 100, user_y, "")
    
    legend_y = 950
    legend_items = [
        (colors['device'], "IoT Devices & Connections"),
        (colors['pipeline'], "Data Ingestion Pipeline"),
        (colors['storage'], "Snowflake Storage Layer"),
        (colors['ai'], "Cortex AI Services"),
        (colors['app'], "Dashboard Components"),
        (colors['user'], "End User"),
    ]
    
    legend_x = 200
    for i, (color, label) in enumerate(legend_items):
        x = legend_x + (i % 3) * 450
        y = legend_y + (i // 3) * 40
        draw.rounded_rectangle([x, y, x+25, y+25], radius=5, fill=color, outline='white')
        draw.text((x + 35, y + 3), label, fill=colors['text'], font=small_font)
    
    draw.text((50, 1100), "Data Flow: Meshtastic devices → Python Streamer → Snowpipe v2 → Snowflake Tables → Cortex Agent → Streamlit Dashboard → User",
              fill=colors['subtext'], font=small_font)
    
    output_path = '/Users/tspann/Downloads/code/coco/meshtastic/architecture_diagram.png'
    img.save(output_path, 'PNG', quality=95)
    print(f"Architecture diagram saved to {output_path}")
    return output_path


if __name__ == "__main__":
    create_architecture_diagram()
