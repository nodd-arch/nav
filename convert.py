# 

import xml.etree.ElementTree as ET
import csv
from pathlib import Path

def parse_coordinates(coord_string):
    """Parse KML coordinate string into list of (lon, lat, alt) tuples."""
    coords = []
    for coord in coord_string.strip().split():
        if coord:
            parts = coord.split(',')
            if len(parts) >= 2:
                lon, lat = parts[0], parts[1]
                coords.append((float(lon), float(lat)))
    return coords

def determine_geometry_type(coords):
    """Determine if geometry is Point, LineString, or Polygon."""
    if len(coords) == 1:
        return 'Point'
    elif coords[0] == coords[-1]:
        return 'Polygon'
    else:
        return 'LineString'

def coords_to_wkt(coords, geom_type):
    """Convert coordinates to WKT (Well-Known Text) format for PostGIS."""
    if geom_type == 'Point':
        return f"POINT({coords[0][0]} {coords[0][1]})"
    elif geom_type == 'LineString':
        coord_str = ', '.join([f"{lon} {lat}" for lon, lat in coords])
        return f"LINESTRING({coord_str})"
    elif geom_type == 'Polygon':
        coord_str = ', '.join([f"{lon} {lat}" for lon, lat in coords])
        return f"POLYGON(({coord_str}))"

def kml_to_csv(kml_path, csv_path):
    """Convert KML file to CSV format suitable for PostgreSQL import."""
    
    # Parse KML
    tree = ET.parse(kml_path)
    root = tree.getroot()
    
    # Define namespace
    ns = {'kml': 'http://www.opengis.net/kml/2.2'}
    
    # Prepare CSV output
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header
        writer.writerow([
            'id', 
            'name', 
            'geometry_type', 
            'geometry_wkt',
            'coordinate_count',
            'lon_min',
            'lon_max',
            'lat_min',
            'lat_max'
        ])
        
        # Process each Placemark
        for placemark in root.findall('.//kml:Placemark', ns):
            # Get ID and name
            placemark_id = placemark.get('id', '')
            name = placemark.find('kml:name', ns)
            name_text = name.text if name is not None else ''
            
            # Try to find Point
            point = placemark.find('.//kml:Point/kml:coordinates', ns)
            if point is not None:
                coords = parse_coordinates(point.text)
                if coords:
                    geom_type = 'Point'
                    wkt = coords_to_wkt(coords, geom_type)
                    
                    writer.writerow([
                        placemark_id,
                        name_text,
                        geom_type,
                        wkt,
                        len(coords),
                        coords[0][0],  # lon_min
                        coords[0][0],  # lon_max
                        coords[0][1],  # lat_min
                        coords[0][1]   # lat_max
                    ])
                continue
            
            # Try to find LineString
            linestring = placemark.find('.//kml:LineString/kml:coordinates', ns)
            if linestring is not None:
                coords = parse_coordinates(linestring.text)
                if coords:
                    geom_type = determine_geometry_type(coords)
                    wkt = coords_to_wkt(coords, geom_type)
                    
                    lons = [c[0] for c in coords]
                    lats = [c[1] for c in coords]
                    
                    writer.writerow([
                        placemark_id,
                        name_text,
                        geom_type,
                        wkt,
                        len(coords),
                        min(lons),
                        max(lons),
                        min(lats),
                        max(lats)
                    ])
                continue
            
            # Try to find Polygon
            polygon = placemark.find('.//kml:Polygon//kml:coordinates', ns)
            if polygon is not None:
                coords = parse_coordinates(polygon.text)
                if coords:
                    geom_type = 'Polygon'
                    wkt = coords_to_wkt(coords, geom_type)
                    
                    lons = [c[0] for c in coords]
                    lats = [c[1] for c in coords]
                    
                    writer.writerow([
                        placemark_id,
                        name_text,
                        geom_type,
                        wkt,
                        len(coords),
                        min(lons),
                        max(lons),
                        min(lats),
                        max(lats)
                    ])

if __name__ == '__main__':
    # Input/Output paths
    kml_file = r"C:\Users\HomePC\Downloads\DEKUT KML.kml"
    csv_file = r"C:\Users\HomePC\Downloads\DEKUT_features.csv"
    
    # Convert
    kml_to_csv(kml_file, csv_file)
    print(f"✓ Conversion complete: {csv_file}")
    print(f"  Ready for PostgreSQL/PostGIS import")