<?php
/**
 * OGN Data API
 * Serves aircraft position data for the map UI
 */

header('Content-Type: application/json');
header('Access-Control-Allow-Origin: *');

// Load env
$envFile = dirname(__DIR__) . '/.env';
if (file_exists($envFile)) {
    foreach (file($envFile, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) as $line) {
        if (str_starts_with(trim($line), '#')) continue;
        if (str_contains($line, '=')) {
            [$key, $val] = explode('=', $line, 2);
            $_ENV[trim($key)] = trim($val);
        }
    }
}

$db = new mysqli(
    $_ENV['DB_HOST'] ?? 'localhost',
    $_ENV['DB_USER'] ?? 'ogn_collector',
    $_ENV['DB_PASS'] ?? '',
    $_ENV['DB_NAME'] ?? 'ogn'
);

if ($db->connect_error) {
    http_response_code(500);
    echo json_encode(['error' => 'Database connection failed']);
    exit;
}

// Stored timestamps are UTC; align session timezone
$db->query("SET time_zone = '+00:00'");

$action = $_GET['action'] ?? 'live';

switch ($action) {

    // Demo mode — fleet flies a left-hand circuit at Unterwoessen, computed from time
    // Runway 22/04 (~220°/040°), circuit on the SE side. Identical response shape to 'live'.
    case 'demo':
        $t = time();

        // Circuit waypoints: [lat, lng, alt_m, leg_duration_secs]
        // Runway 06 (heading 060°/NE), left-hand pattern: circuit to the NW
        // Upwind NE → crosswind NNW → downwind WSW → base SSE → final NE
        $wpts = [
            [47.7260, 12.4325, 608, 40], // 0: Runway 06 threshold (SW end)
            [47.7340, 12.4480, 740, 25], // 1: End of upwind
            [47.7390, 12.4420, 845, 25], // 2: End of crosswind
            [47.7355, 12.4225, 850, 32], // 3: Mid downwind
            [47.7270, 12.4070, 845, 28], // 4: End of downwind
            [47.7215, 12.4150, 790, 25], // 5: End of base
            [47.7245, 12.4290, 680, 32], // 6: Final approach
            [47.7260, 12.4325, 612, 10], // 7: Touchdown
        ];

        // Compute bearing between two lat/lng points
        $bearing = function($lat1, $lng1, $lat2, $lng2) {
            $dL = deg2rad($lng2 - $lng1);
            $y  = sin($dL) * cos(deg2rad($lat2));
            $x  = cos(deg2rad($lat1)) * sin(deg2rad($lat2))
                - sin(deg2rad($lat1)) * cos(deg2rad($lat2)) * cos($dL);
            return fmod(rad2deg(atan2($y, $x)) + 360, 360);
        };

        // Cumulative time offsets (duration is now index 3)
        $cum = [0];
        foreach ($wpts as $w) $cum[] = end($cum) + $w[3];
        $total = end($cum);

        // Fleet: all do the same circuit, evenly spaced in phase
        $fleet = [
            ['reg'=>'D-7507','cn'=>'HM','model'=>'ASK 13',   'type'=>1],
            ['reg'=>'D-1800','cn'=>'HN','model'=>'ASK 13',   'type'=>1],
            ['reg'=>'D-1670','cn'=>'HO','model'=>'ASK 13',   'type'=>1],
            ['reg'=>'D-3982','cn'=>'HP','model'=>'ASK 13',   'type'=>1],
            ['reg'=>'D-1375','cn'=>'HQ','model'=>'ASK 13',   'type'=>1],
            ['reg'=>'D-8474','cn'=>'K1','model'=>'K 8',      'type'=>1],
            ['reg'=>'D-7130','cn'=>'K2','model'=>'K 8',      'type'=>1],
            ['reg'=>'D-5343','cn'=>'K3','model'=>'K 8',      'type'=>1],
            ['reg'=>'D-1864','cn'=>'23','model'=>'ASK 23',   'type'=>1],
            ['reg'=>'D-8999','cn'=>'21','model'=>'ASK 21',   'type'=>1],
            ['reg'=>'D-8250','cn'=>'HF','model'=>'HPH 304',  'type'=>1],
            ['reg'=>'D-8251','cn'=>'L4','model'=>'LS 4',     'type'=>1],
            ['reg'=>'D-2249','cn'=>'DD','model'=>'Duo Discus','type'=>1],
            ['reg'=>'D-1020','cn'=>'K6','model'=>'Ka 6',     'type'=>1],
        ];

        $n = count($fleet);
        $aircraft = [];

        foreach ($fleet as $i => $plane) {
            // Each aircraft evenly spaced around the circuit
            $phase_offset = ($i / $n) * $total;
            $pos = fmod($t + $phase_offset, $total);

            // Find which leg
            $leg = 0;
            for ($j = 0; $j < count($wpts); $j++) {
                if ($pos >= $cum[$j] && $pos < $cum[$j + 1]) { $leg = $j; break; }
            }
            $w0 = $wpts[$leg];
            $w1 = $wpts[($leg + 1) % count($wpts)];
            $f  = ($pos - $cum[$leg]) / $w0[3]; // 0..1 within this leg

            $lat   = $w0[0] + ($w1[0] - $w0[0]) * $f;
            $lng   = $w0[1] + ($w1[1] - $w0[1]) * $f;
            $alt   = $w0[2] + ($w1[2] - $w0[2]) * $f;
            $climb = round(($w1[2] - $w0[2]) / $w0[3] / 1.5, 2);
            $track = (int)round($bearing($w0[0], $w0[1], $w1[0], $w1[1]));

            $aircraft[] = [
                'device_id'        => 'DEMO' . str_pad($i, 3, '0', STR_PAD_LEFT),
                'latitude'         => round($lat, 7),
                'longitude'        => round($lng, 7),
                'altitude_m'       => (int)round($alt),
                'ground_speed_kph' => 90.0,
                'track_deg'        => $track,
                'climb_rate_ms'    => $climb,
                'aircraft_type'    => $plane['type'],
                'received_at'      => date('Y-m-d H:i:s'),
                'signal_db'        => 18.5,
                'registration'     => $plane['reg'],
                'aircraft_model'   => $plane['model'],
                'cn'               => $plane['cn'],
                'tracked'          => 'Y',
                'identified'       => 'Y',
            ];
        }

        echo json_encode([
            'aircraft'        => $aircraft,
            'count'           => count($aircraft),
            'time_window_min' => 10,
            'demo'            => true,
        ]);
        break;

    // Current positions (last 10 minutes, latest per device), enriched with DDB
    case 'live':
        $minutes = intval($_GET['minutes'] ?? 10);
        $minutes = min(max($minutes, 1), 60);
        
        $result = $db->query("
            SELECT p.device_id, p.latitude, p.longitude, p.altitude_m,
                   p.ground_speed_kph, p.track_deg, p.climb_rate_ms,
                   p.aircraft_type, p.received_at, p.signal_db,
                   d.registration, d.aircraft_model, d.cn, d.tracked, d.identified
            FROM ogn_positions p
            INNER JOIN (
                SELECT device_id, MAX(received_at) as max_received
                FROM ogn_positions
                WHERE received_at > DATE_SUB(NOW(), INTERVAL {$minutes} MINUTE)
                GROUP BY device_id
            ) latest ON p.device_id = latest.device_id AND p.received_at = latest.max_received
            LEFT JOIN ogn_ddb d ON p.device_id = d.device_id
            ORDER BY p.received_at DESC
        ");
        
        $aircraft = [];
        while ($row = $result->fetch_assoc()) {
            $row['altitude_m'] = (int)$row['altitude_m'];
            $row['ground_speed_kph'] = (float)$row['ground_speed_kph'];
            $row['track_deg'] = (int)$row['track_deg'];
            $row['climb_rate_ms'] = (float)$row['climb_rate_ms'];
            $row['aircraft_type'] = (int)$row['aircraft_type'];
            $row['latitude'] = (float)$row['latitude'];
            $row['longitude'] = (float)$row['longitude'];
            // Respect DDB privacy flags
            if (($row['identified'] ?? 'Y') === 'N') {
                $row['registration'] = null;
                $row['aircraft_model'] = null;
                $row['cn'] = null;
            }
            $aircraft[] = $row;
        }
        
        echo json_encode([
            'aircraft' => $aircraft,
            'count' => count($aircraft),
            'time_window_min' => $minutes
        ]);
        break;

    // Track for a specific device
    case 'track':
        $deviceId = $db->real_escape_string($_GET['device_id'] ?? '');
        $hours = intval($_GET['hours'] ?? 24);
        $hours = min(max($hours, 1), 168); // max 7 days
        
        if (!$deviceId) {
            echo json_encode(['error' => 'device_id required']);
            break;
        }
        
        $result = $db->query("
            SELECT latitude, longitude, altitude_m, ground_speed_kph,
                   track_deg, climb_rate_ms, received_at
            FROM ogn_positions
            WHERE device_id = '{$deviceId}'
              AND received_at > DATE_SUB(NOW(), INTERVAL {$hours} HOUR)
            ORDER BY received_at ASC
        ");
        
        $points = [];
        while ($row = $result->fetch_assoc()) {
            $row['latitude'] = (float)$row['latitude'];
            $row['longitude'] = (float)$row['longitude'];
            $row['altitude_m'] = (int)$row['altitude_m'];
            $row['ground_speed_kph'] = (float)$row['ground_speed_kph'];
            $row['climb_rate_ms'] = (float)$row['climb_rate_ms'];
            $points[] = $row;
        }
        
        echo json_encode([
            'device_id' => $deviceId,
            'points' => $points,
            'count' => count($points)
        ]);
        break;

    // Stats overview
    case 'stats':
        $stats = [];
        
        // Total positions
        $r = $db->query("SELECT COUNT(*) as total FROM ogn_positions");
        $stats['total_positions'] = (int)$r->fetch_assoc()['total'];
        
        // Unique devices
        $r = $db->query("SELECT COUNT(DISTINCT device_id) as total FROM ogn_positions");
        $stats['unique_devices'] = (int)$r->fetch_assoc()['total'];
        
        // Today's stats
        $r = $db->query("
            SELECT COUNT(*) as positions, COUNT(DISTINCT device_id) as devices
            FROM ogn_positions WHERE DATE(received_at) = CURDATE()
        ");
        $today = $r->fetch_assoc();
        $stats['today_positions'] = (int)$today['positions'];
        $stats['today_devices'] = (int)$today['devices'];
        
        // By aircraft type
        $r = $db->query("
            SELECT aircraft_type, COUNT(*) as cnt, COUNT(DISTINCT device_id) as devices
            FROM ogn_positions GROUP BY aircraft_type ORDER BY cnt DESC
        ");
        $stats['by_type'] = [];
        while ($row = $r->fetch_assoc()) {
            $stats['by_type'][] = $row;
        }
        
        // Daily breakdown (last 30 days)
        $r = $db->query("
            SELECT DATE(received_at) as day, COUNT(*) as positions,
                   COUNT(DISTINCT device_id) as devices
            FROM ogn_positions
            WHERE received_at > DATE_SUB(NOW(), INTERVAL 30 DAY)
            GROUP BY day ORDER BY day DESC
        ");
        $stats['daily'] = [];
        while ($row = $r->fetch_assoc()) {
            $stats['daily'][] = $row;
        }
        
        // Active now (last 10 min)
        $r = $db->query("
            SELECT COUNT(DISTINCT device_id) as active
            FROM ogn_positions
            WHERE received_at > DATE_SUB(NOW(), INTERVAL 10 MINUTE)
        ");
        $stats['active_now'] = (int)$r->fetch_assoc()['active'];
        
        // Database size
        $r = $db->query("
            SELECT ROUND(SUM(data_length + index_length)/1024/1024, 1) as size_mb
            FROM information_schema.tables WHERE table_schema = 'ogn'
        ");
        $stats['db_size_mb'] = (float)$r->fetch_assoc()['size_mb'];
        
        echo json_encode($stats);
        break;

    // Heatmap data (aggregated positions)
    case 'heatmap':
        $days = intval($_GET['days'] ?? 7);
        $days = min(max($days, 1), 90);
        
        $result = $db->query("
            SELECT ROUND(latitude, 2) as lat, ROUND(longitude, 2) as lng,
                   COUNT(*) as weight
            FROM ogn_positions
            WHERE received_at > DATE_SUB(NOW(), INTERVAL {$days} DAY)
            GROUP BY lat, lng
            HAVING weight > 2
            ORDER BY weight DESC
            LIMIT 5000
        ");
        
        $points = [];
        while ($row = $result->fetch_assoc()) {
            $points[] = [
                'lat' => (float)$row['lat'],
                'lng' => (float)$row['lng'],
                'weight' => (int)$row['weight']
            ];
        }
        
        echo json_encode(['points' => $points, 'days' => $days]);
        break;

    // Lookup device by ID or registration
    case 'device':
        $deviceId = $db->real_escape_string($_GET['device_id'] ?? '');
        $reg = $db->real_escape_string($_GET['registration'] ?? '');
        
        if ($deviceId) {
            $result = $db->query("SELECT * FROM ogn_ddb WHERE device_id = '{$deviceId}'");
        } elseif ($reg) {
            $result = $db->query("SELECT * FROM ogn_ddb WHERE registration = '{$reg}'");
        } else {
            echo json_encode(['error' => 'device_id or registration required']);
            break;
        }
        
        $devices = [];
        while ($row = $result->fetch_assoc()) {
            if ($row['identified'] === 'N') {
                $row['registration'] = null;
                $row['aircraft_model'] = null;
                $row['cn'] = null;
            }
            $devices[] = $row;
        }
        echo json_encode(['devices' => $devices, 'count' => count($devices)]);
        break;

    // Nearby aircraft within radius of a point
    case 'nearby':
        $lat = floatval($_GET['lat'] ?? 47.76);
        $lon = floatval($_GET['lon'] ?? 12.46);
        $radiusKm = floatval($_GET['radius'] ?? 50);
        $radiusKm = min(max($radiusKm, 1), 500);
        $minutes = intval($_GET['minutes'] ?? 10);
        $minutes = min(max($minutes, 1), 60);

        // Haversine approximation: 1 degree lat ≈ 111km
        $latDelta = $radiusKm / 111.0;
        $lonDelta = $radiusKm / (111.0 * cos(deg2rad($lat)));

        $result = $db->query("
            SELECT p.device_id, p.latitude, p.longitude, p.altitude_m,
                   p.ground_speed_kph, p.track_deg, p.climb_rate_ms,
                   p.aircraft_type, p.received_at,
                   d.registration, d.aircraft_model, d.cn, d.identified,
                   (6371 * ACOS(
                       COS(RADIANS({$lat})) * COS(RADIANS(p.latitude)) *
                       COS(RADIANS(p.longitude) - RADIANS({$lon})) +
                       SIN(RADIANS({$lat})) * SIN(RADIANS(p.latitude))
                   )) AS distance_km
            FROM ogn_positions p
            INNER JOIN (
                SELECT device_id, MAX(received_at) as max_received
                FROM ogn_positions
                WHERE received_at > DATE_SUB(NOW(), INTERVAL {$minutes} MINUTE)
                  AND latitude BETWEEN {$lat} - {$latDelta} AND {$lat} + {$latDelta}
                  AND longitude BETWEEN {$lon} - {$lonDelta} AND {$lon} + {$lonDelta}
                GROUP BY device_id
            ) latest ON p.device_id = latest.device_id AND p.received_at = latest.max_received
            LEFT JOIN ogn_ddb d ON p.device_id = d.device_id
            HAVING distance_km <= {$radiusKm}
            ORDER BY distance_km ASC
        ");

        $aircraft = [];
        while ($row = $result->fetch_assoc()) {
            $row['latitude'] = (float)$row['latitude'];
            $row['longitude'] = (float)$row['longitude'];
            $row['altitude_m'] = (int)$row['altitude_m'];
            $row['distance_km'] = round((float)$row['distance_km'], 1);
            if (($row['identified'] ?? 'Y') === 'N') {
                $row['registration'] = null;
                $row['aircraft_model'] = null;
                $row['cn'] = null;
            }
            unset($row['identified']);
            $aircraft[] = $row;
        }

        echo json_encode([
            'center' => ['lat' => $lat, 'lon' => $lon],
            'radius_km' => $radiusKm,
            'aircraft' => $aircraft,
            'count' => count($aircraft)
        ]);
        break;

    // DDB stats
    case 'ddb_stats':
        $stats = [];
        $r = $db->query("SELECT COUNT(*) as total FROM ogn_ddb");
        $stats['total_devices'] = (int)$r->fetch_assoc()['total'];
        
        $r = $db->query("SELECT COUNT(*) as total FROM ogn_ddb WHERE registration != ''");
        $stats['with_registration'] = (int)$r->fetch_assoc()['total'];
        
        $r = $db->query("
            SELECT aircraft_type, COUNT(*) as cnt FROM ogn_ddb
            GROUP BY aircraft_type ORDER BY cnt DESC
        ");
        $stats['by_type'] = [];
        while ($row = $r->fetch_assoc()) $stats['by_type'][] = $row;
        
        $r = $db->query("SELECT MAX(updated_at) as last_sync FROM ogn_ddb");
        $stats['last_sync'] = $r->fetch_assoc()['last_sync'];
        
        echo json_encode($stats);
        break;

    // Config (maps key)
    case 'config':
        // Load from gliderincidents .env
        $giEnv = '/opt/gliderincidents/.env';
        $mapsKey = '';
        if (file_exists($giEnv)) {
            foreach (file($giEnv, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) as $line) {
                if (str_starts_with(trim($line), 'GOOGLE_MAPS_API_KEY=')) {
                    $mapsKey = trim(explode('=', $line, 2)[1]);
                    break;
                }
            }
        }
        echo json_encode(['googleMapsApiKey' => $mapsKey]);
        break;

    // Top 20 aircraft models by position count (last 30 days)
    case 'top_models':
        $result = $db->query("
            SELECT d.aircraft_model, COUNT(*) as positions, COUNT(DISTINCT p.device_id) as devices
            FROM ogn_positions p
            JOIN ogn_ddb d ON p.device_id = d.device_id
            WHERE d.aircraft_model != '' AND p.received_at > DATE_SUB(NOW(), INTERVAL 30 DAY)
            GROUP BY d.aircraft_model ORDER BY positions DESC LIMIT 20
        ");
        $models = [];
        while ($row = $result->fetch_assoc()) {
            $row['positions'] = (int)$row['positions'];
            $row['devices'] = (int)$row['devices'];
            $models[] = $row;
        }
        echo json_encode(['models' => $models]);
        break;

    // Top 20 most active devices (last 30 days)
    case 'top_devices':
        $result = $db->query("
            SELECT p.device_id, COUNT(*) as positions, d.registration, d.aircraft_model, d.cn,
                   MIN(p.received_at) as first_seen, MAX(p.received_at) as last_seen
            FROM ogn_positions p
            LEFT JOIN ogn_ddb d ON p.device_id = d.device_id
            WHERE p.received_at > DATE_SUB(NOW(), INTERVAL 30 DAY)
            GROUP BY p.device_id ORDER BY positions DESC LIMIT 20
        ");
        $devices = [];
        while ($row = $result->fetch_assoc()) {
            $row['positions'] = (int)$row['positions'];
            if (($row['identified'] ?? 'Y') === 'N') {
                $row['registration'] = null;
                $row['aircraft_model'] = null;
                $row['cn'] = null;
            }
            unset($row['identified']);
            $devices[] = $row;
        }
        echo json_encode(['devices' => $devices]);
        break;

    // DDB enrichment coverage of tracked devices (last 30 days)
    case 'enrichment_stats':
        $result = $db->query("
            SELECT
              COUNT(DISTINCT p.device_id) as total_tracked,
              COUNT(DISTINCT CASE WHEN d.device_id IS NOT NULL THEN p.device_id END) as ddb_matched,
              COUNT(DISTINCT CASE WHEN d.registration != '' AND d.registration IS NOT NULL THEN p.device_id END) as with_registration,
              COUNT(DISTINCT CASE WHEN d.identified = 'Y' THEN p.device_id END) as identified
            FROM ogn_positions p
            LEFT JOIN ogn_ddb d ON p.device_id = d.device_id
            WHERE p.received_at > DATE_SUB(NOW(), INTERVAL 30 DAY)
        ");
        $row = $result->fetch_assoc();
        $row['total_tracked'] = (int)$row['total_tracked'];
        $row['ddb_matched'] = (int)$row['ddb_matched'];
        $row['with_registration'] = (int)$row['with_registration'];
        $row['identified'] = (int)$row['identified'];
        echo json_encode($row);
        break;

    default:
        echo json_encode(['error' => 'Unknown action', 'actions' => ['live', 'track', 'stats', 'heatmap', 'config', 'top_models', 'top_devices', 'enrichment_stats']]);
}

$db->close();
