import psycopg2
import csv
from pathlib import Path

def create_table(cursor):
    """Create table schema with PostGIS geometry column."""
    cursor.execute("""
        -- Enable PostGIS if not already enabled
        CREATE EXTENSION IF NOT EXISTS postgis;
        
        -- Drop table if exists
        DROP TABLE IF EXISTS dekut_features;
        
        -- Create table
        CREATE TABLE dekut_features (
            id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(255),
            geometry_type VARCHAR(20),
            geom GEOMETRY(Geometry, 4326),  -- WGS84 coordinate system
            coordinate_count INTEGER,
            lon_min DOUBLE PRECISION,
            lon_max DOUBLE PRECISION,
            lat_min DOUBLE PRECISION,
            lat_max DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create spatial index
        CREATE INDEX idx_dekut_features_geom 
        ON dekut_features USING GIST(geom);
        
        -- Create index on geometry type
        CREATE INDEX idx_dekut_features_type 
        ON dekut_features(geometry_type);
    """)

def import_csv_to_postgres(csv_path, db_config):
    """Import CSV data into PostgreSQL."""
    
    # Connect to database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    
    try:
        # Create table
        print("Creating table...")
        create_table(cursor)
        conn.commit()
        
        # Read CSV and insert data
        print("Importing data...")
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            count = 0
            for row in reader:
                cursor.execute("""
                    INSERT INTO dekut_features (
                        id, name, geometry_type, geom,
                        coordinate_count, lon_min, lon_max, lat_min, lat_max
                    )
                    VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s, %s, %s, %s)
                """, (
                    row['id'],
                    row['name'],
                    row['geometry_type'],
                    row['geometry_wkt'],
                    int(row['coordinate_count']),
                    float(row['lon_min']),
                    float(row['lon_max']),
                    float(row['lat_min']),
                    float(row['lat_max'])
                ))
                count += 1
            
            conn.commit()
            print(f"✓ Imported {count} features")
        
        # Print summary statistics
        cursor.execute("""
            SELECT 
                geometry_type,
                COUNT(*) as count
            FROM dekut_features
            GROUP BY geometry_type
            ORDER BY geometry_type
        """)
        
        print("\n--- Feature Summary ---")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]} features")
        
        # Calculate total area covered
        cursor.execute("""
            SELECT 
                ST_AsText(ST_Envelope(ST_Collect(geom))) as bbox,
                ST_Area(ST_Envelope(ST_Collect(geom))::geography) / 1000000 as area_km2
            FROM dekut_features
        """)
        
        bbox, area = cursor.fetchone()
        print(f"\nTotal area covered: {area:.2f} km²")
        
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    # Database configuration
    db_config = {
        'host': 'localhost',
        'database': 'dekutNav',
        'user': 'postgres',
        'password': '12345',
        'port': 5432
    }
    
    # CSV file path
    csv_file = r"D:\Projects\code\offline navigator\dekut-nav\DEKUT_features.csv"
    
    # Import
    import_csv_to_postgres(csv_file, db_config)