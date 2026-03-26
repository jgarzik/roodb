//! Geometry support: WKT parsing, WKB encoding/decoding, and spatial functions.
//!
//! Implements the OGC Simple Features standard for WKB (Well-Known Binary) format.
//! MySQL prepends a 4-byte SRID to the standard WKB; we use SRID=0 by default.
//!
//! WKB format (little-endian):
//!   byte_order: u8 (0x01 = little-endian)
//!   type: u32 (1=Point, 2=LineString, 3=Polygon, 4=MultiPoint,
//!              5=MultiLineString, 6=MultiPolygon, 7=GeometryCollection)
//!   coordinates: type-specific coordinate data (f64 pairs)

use std::io::{Cursor, Read};

// WKB geometry type codes
const WKB_POINT: u32 = 1;
const WKB_LINESTRING: u32 = 2;
const WKB_POLYGON: u32 = 3;
const WKB_MULTILINESTRING: u32 = 5;
const WKB_MULTIPOLYGON: u32 = 6;

// MySQL SRID prefix length
const SRID_LEN: usize = 4;

/// Parse a WKT (Well-Known Text) string into MySQL WKB binary (with 4-byte SRID prefix).
pub fn wkt_to_wkb(wkt: &str) -> Result<Vec<u8>, String> {
    let wkt = wkt.trim();
    let upper = wkt.to_uppercase();

    // Parse type keyword and coordinates
    if let Some(rest) = strip_prefix_ci(&upper, wkt, "POINT") {
        let coords = parse_parens(rest)?;
        let pt = parse_point_coords(&coords)?;
        Ok(encode_point(pt.0, pt.1))
    } else if let Some(rest) = strip_prefix_ci(&upper, wkt, "MULTIPOLYGON") {
        let inner = parse_parens(rest)?;
        encode_multipolygon(&inner)
    } else if let Some(rest) = strip_prefix_ci(&upper, wkt, "MULTILINESTRING") {
        let inner = parse_parens(rest)?;
        encode_multilinestring(&inner)
    } else if let Some(rest) = strip_prefix_ci(&upper, wkt, "POLYGON") {
        let inner = parse_parens(rest)?;
        encode_polygon(&inner)
    } else if let Some(rest) = strip_prefix_ci(&upper, wkt, "LINESTRING") {
        let coords = parse_parens(rest)?;
        encode_linestring(&coords)
    } else {
        Err(format!("Unsupported geometry type in WKT: {}", wkt))
    }
}

/// Extract the X coordinate from a POINT geometry (MySQL WKB with SRID).
pub fn st_x(wkb: &[u8]) -> Result<f64, String> {
    let data = skip_srid(wkb)?;
    let mut cur = Cursor::new(data);
    let _order = read_u8(&mut cur)?;
    let geom_type = read_u32_le(&mut cur)?;
    if geom_type != WKB_POINT {
        return Err("ST_X requires a POINT geometry".to_string());
    }
    let x = read_f64_le(&mut cur)?;
    Ok(x)
}

/// Extract the Y coordinate from a POINT geometry.
pub fn st_y(wkb: &[u8]) -> Result<f64, String> {
    let data = skip_srid(wkb)?;
    let mut cur = Cursor::new(data);
    let _order = read_u8(&mut cur)?;
    let geom_type = read_u32_le(&mut cur)?;
    if geom_type != WKB_POINT {
        return Err("ST_Y requires a POINT geometry".to_string());
    }
    let _x = read_f64_le(&mut cur)?;
    let y = read_f64_le(&mut cur)?;
    Ok(y)
}

/// Count the number of points in a LINESTRING geometry.
pub fn st_numpoints(wkb: &[u8]) -> Result<i64, String> {
    let data = skip_srid(wkb)?;
    let mut cur = Cursor::new(data);
    let _order = read_u8(&mut cur)?;
    let geom_type = read_u32_le(&mut cur)?;
    if geom_type != WKB_LINESTRING {
        return Err("ST_NumPoints requires a LINESTRING geometry".to_string());
    }
    let num_points = read_u32_le(&mut cur)?;
    Ok(num_points as i64)
}

/// Compute the length of a LINESTRING or total length of a MULTILINESTRING.
pub fn st_length(wkb: &[u8]) -> Result<f64, String> {
    let data = skip_srid(wkb)?;
    let mut cur = Cursor::new(data);
    let _order = read_u8(&mut cur)?;
    let geom_type = read_u32_le(&mut cur)?;

    match geom_type {
        WKB_LINESTRING => linestring_length(&mut cur),
        WKB_MULTILINESTRING => {
            let num_lines = read_u32_le(&mut cur)?;
            let mut total = 0.0;
            for _ in 0..num_lines {
                let _order = read_u8(&mut cur)?;
                let _type = read_u32_le(&mut cur)?;
                total += linestring_length(&mut cur)?;
            }
            Ok(total)
        }
        _ => Err("ST_Length requires a LINESTRING or MULTILINESTRING".to_string()),
    }
}

/// Compute the area of a POLYGON or MULTIPOLYGON.
pub fn st_area(wkb: &[u8]) -> Result<f64, String> {
    let data = skip_srid(wkb)?;
    let mut cur = Cursor::new(data);
    let _order = read_u8(&mut cur)?;
    let geom_type = read_u32_le(&mut cur)?;

    match geom_type {
        WKB_POLYGON => polygon_area(&mut cur),
        WKB_MULTIPOLYGON => {
            let num_polys = read_u32_le(&mut cur)?;
            let mut total = 0.0;
            for _ in 0..num_polys {
                let _order = read_u8(&mut cur)?;
                let _type = read_u32_le(&mut cur)?;
                total += polygon_area(&mut cur)?;
            }
            Ok(total)
        }
        _ => Err("ST_Area requires a POLYGON or MULTIPOLYGON".to_string()),
    }
}

// ============ WKB encoding ============

fn encode_point(x: f64, y: f64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(SRID_LEN + 1 + 4 + 16);
    buf.extend_from_slice(&0u32.to_le_bytes()); // SRID
    buf.push(0x01); // little-endian
    buf.extend_from_slice(&WKB_POINT.to_le_bytes());
    buf.extend_from_slice(&x.to_le_bytes());
    buf.extend_from_slice(&y.to_le_bytes());
    buf
}

fn encode_linestring(coords_str: &str) -> Result<Vec<u8>, String> {
    let points = parse_coord_list(coords_str)?;
    let mut buf = Vec::new();
    buf.extend_from_slice(&0u32.to_le_bytes()); // SRID
    buf.push(0x01);
    buf.extend_from_slice(&WKB_LINESTRING.to_le_bytes());
    buf.extend_from_slice(&(points.len() as u32).to_le_bytes());
    for (x, y) in &points {
        buf.extend_from_slice(&x.to_le_bytes());
        buf.extend_from_slice(&y.to_le_bytes());
    }
    Ok(buf)
}

fn encode_polygon(coords_str: &str) -> Result<Vec<u8>, String> {
    let rings = parse_ring_list(coords_str)?;
    let mut buf = Vec::new();
    buf.extend_from_slice(&0u32.to_le_bytes()); // SRID
    buf.push(0x01);
    buf.extend_from_slice(&WKB_POLYGON.to_le_bytes());
    buf.extend_from_slice(&(rings.len() as u32).to_le_bytes());
    for ring in &rings {
        buf.extend_from_slice(&(ring.len() as u32).to_le_bytes());
        for (x, y) in ring {
            buf.extend_from_slice(&x.to_le_bytes());
            buf.extend_from_slice(&y.to_le_bytes());
        }
    }
    Ok(buf)
}

fn encode_multilinestring(coords_str: &str) -> Result<Vec<u8>, String> {
    let rings = parse_ring_list(coords_str)?;
    let mut buf = Vec::new();
    buf.extend_from_slice(&0u32.to_le_bytes()); // SRID
    buf.push(0x01);
    buf.extend_from_slice(&WKB_MULTILINESTRING.to_le_bytes());
    buf.extend_from_slice(&(rings.len() as u32).to_le_bytes());
    for ring in &rings {
        buf.push(0x01); // each linestring: byte order
        buf.extend_from_slice(&WKB_LINESTRING.to_le_bytes());
        buf.extend_from_slice(&(ring.len() as u32).to_le_bytes());
        for (x, y) in ring {
            buf.extend_from_slice(&x.to_le_bytes());
            buf.extend_from_slice(&y.to_le_bytes());
        }
    }
    Ok(buf)
}

fn encode_multipolygon(coords_str: &str) -> Result<Vec<u8>, String> {
    // MULTIPOLYGON(((ring1),(ring2)),((ring3)))
    // Split into polygon groups by finding matching parens
    let polygons = split_top_level_parens(coords_str)?;
    let mut buf = Vec::new();
    buf.extend_from_slice(&0u32.to_le_bytes()); // SRID
    buf.push(0x01);
    buf.extend_from_slice(&WKB_MULTIPOLYGON.to_le_bytes());
    buf.extend_from_slice(&(polygons.len() as u32).to_le_bytes());
    for poly_str in &polygons {
        let rings = parse_ring_list(poly_str)?;
        buf.push(0x01); // byte order
        buf.extend_from_slice(&WKB_POLYGON.to_le_bytes());
        buf.extend_from_slice(&(rings.len() as u32).to_le_bytes());
        for ring in &rings {
            buf.extend_from_slice(&(ring.len() as u32).to_le_bytes());
            for (x, y) in ring {
                buf.extend_from_slice(&x.to_le_bytes());
                buf.extend_from_slice(&y.to_le_bytes());
            }
        }
    }
    Ok(buf)
}

// ============ WKT parsing helpers ============

/// Case-insensitive prefix strip that returns the remaining original-case string
fn strip_prefix_ci<'a>(upper: &str, original: &'a str, prefix: &str) -> Option<&'a str> {
    if upper.starts_with(prefix) {
        Some(original[prefix.len()..].trim())
    } else {
        None
    }
}

/// Extract content inside outermost parentheses: "(content)" → "content"
fn parse_parens(s: &str) -> Result<String, String> {
    let s = s.trim();
    if s.starts_with('(') && s.ends_with(')') {
        Ok(s[1..s.len() - 1].to_string())
    } else {
        Err(format!("Expected parenthesized coordinates, got: {}", s))
    }
}

/// Parse "x y" into (f64, f64)
fn parse_point_coords(s: &str) -> Result<(f64, f64), String> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 2 {
        return Err(format!("Expected 2 coordinates, got: {}", s));
    }
    let x = parts[0]
        .parse::<f64>()
        .map_err(|e| format!("Invalid X: {}", e))?;
    let y = parts[1]
        .parse::<f64>()
        .map_err(|e| format!("Invalid Y: {}", e))?;
    Ok((x, y))
}

/// Parse "x1 y1,x2 y2,..." into Vec<(f64, f64)>
fn parse_coord_list(s: &str) -> Result<Vec<(f64, f64)>, String> {
    s.split(',').map(|p| parse_point_coords(p.trim())).collect()
}

/// Parse "(x1 y1,...),(x2 y2,...)" into Vec<Vec<(f64,f64)>> (list of rings)
fn parse_ring_list(s: &str) -> Result<Vec<Vec<(f64, f64)>>, String> {
    let groups = split_top_level_parens(s)?;
    groups.iter().map(|g| parse_coord_list(g)).collect()
}

/// Split a string by top-level parenthesized groups.
/// "(a,b),(c,d)" → ["a,b", "c,d"]
fn split_top_level_parens(s: &str) -> Result<Vec<String>, String> {
    let s = s.trim();
    let mut groups = Vec::new();
    let mut depth = 0i32;
    let mut start = None;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => {
                if depth == 0 {
                    start = Some(i + 1);
                }
                depth += 1;
            }
            ')' => {
                depth -= 1;
                if depth == 0 {
                    if let Some(s_idx) = start {
                        groups.push(s[s_idx..i].to_string());
                    }
                    start = None;
                }
            }
            _ => {}
        }
    }
    if groups.is_empty() {
        return Err(format!("No parenthesized groups found in: {}", s));
    }
    Ok(groups)
}

// ============ WKB reading helpers ============

fn skip_srid(wkb: &[u8]) -> Result<&[u8], String> {
    if wkb.len() < SRID_LEN + 5 {
        return Err("WKB too short".to_string());
    }
    Ok(&wkb[SRID_LEN..])
}

fn read_u8(cur: &mut Cursor<&[u8]>) -> Result<u8, String> {
    let mut buf = [0u8; 1];
    cur.read_exact(&mut buf)
        .map_err(|e| format!("WKB read error: {}", e))?;
    Ok(buf[0])
}

fn read_u32_le(cur: &mut Cursor<&[u8]>) -> Result<u32, String> {
    let mut buf = [0u8; 4];
    cur.read_exact(&mut buf)
        .map_err(|e| format!("WKB read error: {}", e))?;
    Ok(u32::from_le_bytes(buf))
}

fn read_f64_le(cur: &mut Cursor<&[u8]>) -> Result<f64, String> {
    let mut buf = [0u8; 8];
    cur.read_exact(&mut buf)
        .map_err(|e| format!("WKB read error: {}", e))?;
    Ok(f64::from_le_bytes(buf))
}

// ============ Geometry computation helpers ============

fn linestring_length(cur: &mut Cursor<&[u8]>) -> Result<f64, String> {
    let num_points = read_u32_le(cur)?;
    if num_points == 0 {
        return Ok(0.0);
    }
    let mut prev_x = read_f64_le(cur)?;
    let mut prev_y = read_f64_le(cur)?;
    let mut total = 0.0;
    for _ in 1..num_points {
        let x = read_f64_le(cur)?;
        let y = read_f64_le(cur)?;
        let dx = x - prev_x;
        let dy = y - prev_y;
        total += (dx * dx + dy * dy).sqrt();
        prev_x = x;
        prev_y = y;
    }
    Ok(total)
}

/// Compute polygon area using the shoelace formula.
/// Returns absolute area of exterior ring minus holes.
fn polygon_area(cur: &mut Cursor<&[u8]>) -> Result<f64, String> {
    let num_rings = read_u32_le(cur)?;
    let mut total_area = 0.0;
    for ring_idx in 0..num_rings {
        let num_points = read_u32_le(cur)?;
        let mut points = Vec::with_capacity(num_points as usize);
        for _ in 0..num_points {
            let x = read_f64_le(cur)?;
            let y = read_f64_le(cur)?;
            points.push((x, y));
        }
        let area = shoelace_area(&points).abs();
        if ring_idx == 0 {
            total_area += area; // exterior ring
        } else {
            total_area -= area; // interior rings (holes)
        }
    }
    Ok(total_area)
}

/// Shoelace formula for polygon area (signed).
fn shoelace_area(points: &[(f64, f64)]) -> f64 {
    let n = points.len();
    if n < 3 {
        return 0.0;
    }
    let mut area = 0.0;
    for i in 0..n {
        let j = (i + 1) % n;
        area += points[i].0 * points[j].1;
        area -= points[j].0 * points[i].1;
    }
    area / 2.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_roundtrip() {
        let wkb = wkt_to_wkb("POINT(10 20)").unwrap();
        assert_eq!(st_x(&wkb).unwrap(), 10.0);
        assert_eq!(st_y(&wkb).unwrap(), 20.0);
    }

    #[test]
    fn test_linestring_numpoints() {
        let wkb = wkt_to_wkb("LINESTRING(0 0,5 5,6 6)").unwrap();
        assert_eq!(st_numpoints(&wkb).unwrap(), 3);
    }

    #[test]
    fn test_linestring_length() {
        let wkb = wkt_to_wkb("LINESTRING(0 0,3 4)").unwrap();
        let len = st_length(&wkb).unwrap();
        assert!((len - 5.0).abs() < 1e-10); // 3-4-5 triangle
    }

    #[test]
    fn test_polygon_area() {
        // Unit square
        let wkb = wkt_to_wkb("POLYGON((0 0,1 0,1 1,0 1,0 0))").unwrap();
        let area = st_area(&wkb).unwrap();
        assert!((area - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_multipolygon_area() {
        let wkb = wkt_to_wkb("MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)),((2 2,4 5,6 2,2 2)))").unwrap();
        let area = st_area(&wkb).unwrap();
        // First polygon: 5x5 = 25
        // Second polygon: triangle with vertices (2,2),(4,5),(6,2) area = 6
        assert!((area - 31.0).abs() < 1e-10);
    }
}
