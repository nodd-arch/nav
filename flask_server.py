from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import json

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend requests

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'dekutNav',
    'user': 'postgres',
    'password': '12345',
    'port': 5432
}

def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(**DB_CONFIG)

@app.route('/api/features', methods=['GET'])
def get_all_features():
    """Get all features as GeoJSON."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get filter parameters
        geometry_type = request.args.get('type', None)
        
        # Build query
        query = """
            SELECT 
                id,
                name,
                geometry_type,
                ST_AsGeoJSON(geom) as geometry,
                coordinate_count,
                lon_min,
                lon_max,
                lat_min,
                lat_max
            FROM dekut_features
        """
        
        params = []
        if geometry_type:
            query += " WHERE geometry_type = %s"
            params.append(geometry_type)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # Convert to GeoJSON FeatureCollection
        features = []
        for row in rows:
            feature = {
                "type": "Feature",
                "id": row['id'],
                "geometry": json.loads(row['geometry']),
                "properties": {
                    "id": row['id'],
                    "name": row['name'],
                    "geometry_type": row['geometry_type'],
                    "coordinate_count": row['coordinate_count'],
                    "bounds": {
                        "lon_min": row['lon_min'],
                        "lon_max": row['lon_max'],
                        "lat_min": row['lat_min'],
                        "lat_max": row['lat_max']
                    }
                }
            }
            features.append(feature)
        
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        
        cursor.close()
        conn.close()
        
        return jsonify(geojson)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/features/polygons', methods=['GET'])
def get_polygons():
    """Get only polygon features (buildings, areas)."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT 
                id,
                name,
                geometry_type,
                ST_AsGeoJSON(geom) as geometry,
                ST_Area(geom::geography) as area_m2
            FROM dekut_features
            WHERE geometry_type = 'Polygon'
        """)
        
        rows = cursor.fetchall()
        
        features = []
        for row in rows:
            feature = {
                "type": "Feature",
                "id": row['id'],
                "geometry": json.loads(row['geometry']),
                "properties": {
                    "id": row['id'],
                    "name": row['name'],
                    "geometry_type": row['geometry_type'],
                    "area_m2": float(row['area_m2']) if row['area_m2'] else None
                }
            }
            features.append(feature)
        
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        
        cursor.close()
        conn.close()
        
        return jsonify(geojson)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/features/linestrings', methods=['GET'])
def get_linestrings():
    """Get only linestring features (roads, paths)."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT 
                id,
                name,
                geometry_type,
                ST_AsGeoJSON(geom) as geometry,
                ST_Length(geom::geography) as length_m
            FROM dekut_features
            WHERE geometry_type = 'LineString'
        """)
        
        rows = cursor.fetchall()
        
        features = []
        for row in rows:
            feature = {
                "type": "Feature",
                "id": row['id'],
                "geometry": json.loads(row['geometry']),
                "properties": {
                    "id": row['id'],
                    "name": row['name'],
                    "geometry_type": row['geometry_type'],
                    "length_m": float(row['length_m']) if row['length_m'] else None
                }
            }
            features.append(feature)
        
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        
        cursor.close()
        conn.close()
        
        return jsonify(geojson)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/features/points', methods=['GET'])
def get_points():
    """Get only point features (gates, landmarks)."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT 
                id,
                name,
                geometry_type,
                ST_AsGeoJSON(geom) as geometry,
                ST_X(geom) as longitude,
                ST_Y(geom) as latitude
            FROM dekut_features
            WHERE geometry_type = 'Point'
        """)
        
        rows = cursor.fetchall()
        
        features = []
        for row in rows:
            feature = {
                "type": "Feature",
                "id": row['id'],
                "geometry": json.loads(row['geometry']),
                "properties": {
                    "id": row['id'],
                    "name": row['name'],
                    "geometry_type": row['geometry_type'],
                    "longitude": row['longitude'],
                    "latitude": row['latitude']
                }
            }
            features.append(feature)
        
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        
        cursor.close()
        conn.close()
        
        return jsonify(geojson)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/search', methods=['GET'])
def search_features():
    """Search features by name."""
    try:
        query = request.args.get('q', '').strip()
        
        if not query:
            return jsonify({"error": "Query parameter 'q' is required"}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT 
                id,
                name,
                geometry_type,
                ST_AsGeoJSON(ST_Centroid(geom)) as centroid,
                ST_X(ST_Centroid(geom)) as longitude,
                ST_Y(ST_Centroid(geom)) as latitude
            FROM dekut_features
            WHERE LOWER(name) LIKE LOWER(%s)
            ORDER BY name
            LIMIT 10
        """, (f'%{query}%',))
        
        rows = cursor.fetchall()
        
        results = []
        for row in rows:
            results.append({
                "id": row['id'],
                "name": row['name'],
                "type": row['geometry_type'],
                "longitude": row['longitude'],
                "latitude": row['latitude'],
                "centroid": json.loads(row['centroid'])
            })
        
        cursor.close()
        conn.close()
        
        return jsonify(results)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get database statistics."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT 
                geometry_type,
                COUNT(*) as count
            FROM dekut_features
            GROUP BY geometry_type
            ORDER BY geometry_type
        """)
        
        type_counts = cursor.fetchall()
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total_features,
                ST_AsGeoJSON(ST_Envelope(ST_Collect(geom))) as bbox
            FROM dekut_features
        """)
        
        stats = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            "total_features": stats['total_features'],
            "by_type": [dict(row) for row in type_counts],
            "bounding_box": json.loads(stats['bbox']) if stats['bbox'] else None
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/route', methods=['POST'])
def calculate_route():
    """Calculate route between two points (placeholder for future routing)."""
    try:
        data = request.get_json()
        start_id = data.get('start_id')
        end_id = data.get('end_id')
        
        if not start_id or not end_id:
            return jsonify({"error": "start_id and end_id are required"}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get centroids of both features
        cursor.execute("""
            SELECT 
                id,
                name,
                ST_X(ST_Centroid(geom)) as longitude,
                ST_Y(ST_Centroid(geom)) as latitude
            FROM dekut_features
            WHERE id IN (%s, %s)
        """, (start_id, end_id))
        
        points = cursor.fetchall()
        
        if len(points) != 2:
            return jsonify({"error": "One or both features not found"}), 404
        
        start = next(p for p in points if p['id'] == start_id)
        end = next(p for p in points if p['id'] == end_id)
        
        # Simple straight line route (replace with pgRouting in production)
        route = {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    [start['longitude'], start['latitude']],
                    [end['longitude'], end['latitude']]
                ]
            },
            "properties": {
                "from": start['name'],
                "to": end['name'],
                "distance_m": None  # Calculate with ST_Distance in production
            }
        }
        
        cursor.close()
        conn.close()
        
        return jsonify(route)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        return jsonify({"status": "healthy", "database": "connected"})
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500

if __name__ == '__main__':
    print("Starting DEKUT GIS API Server...")
    # print("API endpoints:")
    # print("  GET  /api/features - All features")
    # print("  GET  /api/features/polygons - Buildings/areas")
    # print("  GET  /api/features/linestrings - Roads/paths")
    # print("  GET  /api/features/points - Gates/landmarks")
    # print("  GET  /api/search?q=<query> - Search features")
    # print("  GET  /api/stats - Database statistics")
    # print("  POST /api/route - Calculate route")
    # print("  GET  /api/health - Health check")
    # print("\nServer running on http://localhost:5000")
    
    app.run(debug=True, host='0.0.0.0', port=5000)