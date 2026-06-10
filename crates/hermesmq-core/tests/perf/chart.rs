use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

const WIDTH: usize = 1180;
const LABEL_X: usize = 56;
const BARS_X0: usize = 490;
const BARS_X1: usize = 1000;
const AXIS_MIN: f64 = 100.0;
const ROW_H: usize = 28;
const BAR_H: usize = 16;

const BG: Rgb = (13, 17, 23);
const TITLE_COLOR: Rgb = (240, 246, 252);
const SUBTITLE_COLOR: Rgb = (139, 148, 158);
const LABEL_COLOR: Rgb = (201, 209, 217);
const GRID_COLOR: Rgb = (38, 44, 52);
const TRACK_COLOR: Rgb = (24, 29, 36);

const SECTION_COLORS: [Rgb; 3] = [(88, 166, 255), (63, 185, 80), (188, 140, 255)];
const SECTION_TITLES: [&str; 3] = [
    "End-to-end TCP, single node, fsync on every commit",
    "3-node replicated cluster, in-memory store",
    "Queue state machine, in-process, no I/O",
];

const SPECS: [(&str, &str, &str, usize); 10] = [
    ("tcp_seq", "produce, sequential", "msg/s", 0),
    ("tcp_conc", "produce, 4 connections", "msg/s", 0),
    ("tcp_pipe", "produce, pipelined x32", "msg/s", 0),
    ("tcp_drain", "poll(256) + ack each", "msg/s", 0),
    ("tcp_sub", "subscribe push + ack", "msg/s", 0),
    ("cl_seq", "produce, sequential", "msg/s", 1),
    ("cl_conc", "produce, 8 concurrent writers", "msg/s", 1),
    ("q_produce", "produce", "ops/s", 2),
    ("q_drain", "poll + ack drain, batch 256", "msg/s", 2),
    ("q_hd", "has_deliverable, 20k backlog", "calls/s", 2),
];

type Rgb = (u8, u8, u8);
type Row<'a> = (&'a str, &'a Metric, &'a str);

#[derive(Default, Serialize, Deserialize)]
struct Store {
    metrics: BTreeMap<String, Metric>,
}

#[derive(Serialize, Deserialize)]
struct Metric {
    value: f64,
    note: Option<String>,
}

static LOCK: Mutex<()> = Mutex::new(());

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..").join("..")
}

fn store_path() -> PathBuf {
    root().join("target").join("perf-metrics.json")
}

fn load() -> Store {
    std::fs::read(store_path())
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

pub fn record(key: &str, value: f64, note: Option<String>) {
    let _guard = LOCK.lock().unwrap();
    let mut store = load();
    store.metrics.insert(key.to_string(), Metric { value, note });
    if let Ok(bytes) = serde_json::to_vec_pretty(&store) {
        let _ = std::fs::write(store_path(), bytes);
    }
}

pub fn render() {
    let _guard = LOCK.lock().unwrap();
    let store = load();
    if store.metrics.is_empty() {
        return;
    }

    let mut sections: Vec<(usize, Vec<Row<'_>>)> = Vec::new();
    for section in 0..SECTION_TITLES.len() {
        let rows: Vec<Row<'_>> = SPECS
            .iter()
            .filter(|(_, _, _, s)| *s == section)
            .filter_map(|(key, label, unit, _)| {
                store.metrics.get(*key).map(|m| (*label, m, *unit))
            })
            .collect();
        if !rows.is_empty() {
            sections.push((section, rows));
        }
    }
    if sections.is_empty() {
        return;
    }

    let max_value = sections
        .iter()
        .flat_map(|(_, rows)| rows.iter().map(|(_, m, _)| m.value))
        .fold(AXIS_MIN, f64::max);
    let axis_max = 10f64.powf(max_value.log10().ceil()).max(1000.0);

    let header_h = 96;
    let section_h: usize = sections
        .iter()
        .map(|(_, rows)| 34 + rows.len() * ROW_H + 14)
        .sum();
    let axis_h = 34;
    let height = header_h + section_h + axis_h;

    let mut canvas = Canvas::new(WIDTH, height, BG);

    canvas.text(LABEL_X, 26, "HermesMQ performance", 3, TITLE_COLOR);
    canvas.text(
        LABEL_X,
        58,
        "messages per second, log scale - release build - regenerate with: cargo perf",
        2,
        SUBTITLE_COLOR,
    );

    let chart_top = header_h;
    let chart_bottom = header_h + section_h;
    let mut decade = 1000.0;
    while decade <= axis_max {
        let x = axis_x(decade, axis_max);
        canvas.vline(x, chart_top, chart_bottom, GRID_COLOR);
        let label = fmt_axis(decade);
        canvas.text(
            x.saturating_sub(label.len() * 6),
            chart_bottom + 10,
            &label,
            2,
            SUBTITLE_COLOR,
        );
        decade *= 10.0;
    }

    let mut y = header_h;
    for (section, rows) in sections {
        let color = SECTION_COLORS[section];
        canvas.fill_rect(LABEL_X, y + 4, 10, 10, color);
        canvas.text(LABEL_X + 20, y, SECTION_TITLES[section], 2, LABEL_COLOR);
        y += 34;
        for (label, metric, unit) in rows {
            let bar_y = y + (ROW_H - BAR_H) / 2;
            canvas.text(LABEL_X + 20, bar_y + 1, label, 2, LABEL_COLOR);
            canvas.fill_rect(BARS_X0, bar_y, BARS_X1 - BARS_X0, BAR_H, TRACK_COLOR);
            let bar_end = axis_x(metric.value.max(AXIS_MIN), axis_max).max(BARS_X0 + 3);
            canvas.bar(BARS_X0, bar_y, bar_end - BARS_X0, BAR_H, color);
            let mut value_text = format!("{} {unit}", fmt_value(metric.value));
            if let Some(note) = &metric.note {
                value_text.push_str(&format!("  ({note})"));
            }
            canvas.text(bar_end + 10, bar_y + 1, &value_text, 2, lighten(color, 0.55));
            y += ROW_H;
        }
        y += 14;
    }

    let png = encode_png(canvas.width, canvas.height, &canvas.pixels);
    let path = root().join("performance.png");
    match std::fs::write(&path, png) {
        Ok(()) => println!("performance chart written to {}", path.display()),
        Err(e) => eprintln!("could not write performance chart: {e}"),
    }
}

fn axis_x(value: f64, axis_max: f64) -> usize {
    let span = axis_max.log10() - AXIS_MIN.log10();
    let frac = (value.log10() - AXIS_MIN.log10()) / span;
    BARS_X0 + (frac.clamp(0.0, 1.0) * (BARS_X1 - BARS_X0) as f64) as usize
}

fn fmt_value(v: f64) -> String {
    if v >= 1e6 {
        format!("{:.1}M", v / 1e6)
    } else {
        thousands(v.round() as u64)
    }
}

fn fmt_axis(v: f64) -> String {
    if v >= 1e6 {
        format!("{}M", (v / 1e6) as u64)
    } else {
        format!("{}k", (v / 1e3) as u64)
    }
}

fn thousands(mut v: u64) -> String {
    let mut parts = Vec::new();
    while v >= 1000 {
        parts.push(format!("{:03}", v % 1000));
        v /= 1000;
    }
    parts.push(v.to_string());
    parts.reverse();
    parts.join(",")
}

fn lighten(c: Rgb, amount: f64) -> Rgb {
    let mix = |v: u8| (v as f64 + (255.0 - v as f64) * amount) as u8;
    (mix(c.0), mix(c.1), mix(c.2))
}

struct Canvas {
    width: usize,
    height: usize,
    pixels: Vec<u8>,
}

impl Canvas {
    fn new(width: usize, height: usize, bg: Rgb) -> Self {
        let mut pixels = vec![0u8; width * height * 3];
        for p in pixels.chunks_exact_mut(3) {
            p[0] = bg.0;
            p[1] = bg.1;
            p[2] = bg.2;
        }
        Self { width, height, pixels }
    }

    fn set(&mut self, x: usize, y: usize, c: Rgb) {
        if x >= self.width || y >= self.height {
            return;
        }
        let i = (y * self.width + x) * 3;
        self.pixels[i] = c.0;
        self.pixels[i + 1] = c.1;
        self.pixels[i + 2] = c.2;
    }

    fn fill_rect(&mut self, x: usize, y: usize, w: usize, h: usize, c: Rgb) {
        for dy in 0..h {
            for dx in 0..w {
                self.set(x + dx, y + dy, c);
            }
        }
    }

    fn vline(&mut self, x: usize, y0: usize, y1: usize, c: Rgb) {
        for y in y0..y1 {
            self.set(x, y, c);
        }
    }

    fn bar(&mut self, x: usize, y: usize, w: usize, h: usize, c: Rgb) {
        for dy in 0..h {
            let frac = dy as f64 / h as f64;
            let shade = 1.0 - 0.35 * frac;
            let color = (
                (c.0 as f64 * shade) as u8,
                (c.1 as f64 * shade) as u8,
                (c.2 as f64 * shade) as u8,
            );
            for dx in 0..w {
                self.set(x + dx, y + dy, color);
            }
        }
        self.fill_rect(x, y, w, 1, lighten(c, 0.35));
    }

    fn text(&mut self, x: usize, y: usize, s: &str, scale: usize, c: Rgb) {
        let mut cx = x;
        for ch in s.chars() {
            let rows = glyph(ch);
            for (ry, row) in rows.iter().enumerate() {
                for bit in 0..5 {
                    if row & (0x10 >> bit) != 0 {
                        self.fill_rect(cx + bit * scale, y + ry * scale, scale, scale, c);
                    }
                }
            }
            cx += 6 * scale;
        }
    }
}

fn glyph(c: char) -> [u8; 7] {
    match c {
        'A' => [0x0E, 0x11, 0x11, 0x1F, 0x11, 0x11, 0x11],
        'B' => [0x1E, 0x11, 0x11, 0x1E, 0x11, 0x11, 0x1E],
        'C' => [0x0E, 0x11, 0x10, 0x10, 0x10, 0x11, 0x0E],
        'D' => [0x1C, 0x12, 0x11, 0x11, 0x11, 0x12, 0x1C],
        'E' => [0x1F, 0x10, 0x10, 0x1E, 0x10, 0x10, 0x1F],
        'F' => [0x1F, 0x10, 0x10, 0x1E, 0x10, 0x10, 0x10],
        'G' => [0x0E, 0x11, 0x10, 0x13, 0x11, 0x11, 0x0F],
        'H' => [0x11, 0x11, 0x11, 0x1F, 0x11, 0x11, 0x11],
        'I' => [0x0E, 0x04, 0x04, 0x04, 0x04, 0x04, 0x0E],
        'J' => [0x07, 0x02, 0x02, 0x02, 0x02, 0x12, 0x0C],
        'K' => [0x11, 0x12, 0x14, 0x18, 0x14, 0x12, 0x11],
        'L' => [0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x1F],
        'M' => [0x11, 0x1B, 0x15, 0x15, 0x11, 0x11, 0x11],
        'N' => [0x11, 0x19, 0x15, 0x13, 0x11, 0x11, 0x11],
        'O' => [0x0E, 0x11, 0x11, 0x11, 0x11, 0x11, 0x0E],
        'P' => [0x1E, 0x11, 0x11, 0x1E, 0x10, 0x10, 0x10],
        'Q' => [0x0E, 0x11, 0x11, 0x11, 0x15, 0x12, 0x0D],
        'R' => [0x1E, 0x11, 0x11, 0x1E, 0x14, 0x12, 0x11],
        'S' => [0x0F, 0x10, 0x10, 0x0E, 0x01, 0x01, 0x1E],
        'T' => [0x1F, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04],
        'U' => [0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x0E],
        'V' => [0x11, 0x11, 0x11, 0x11, 0x11, 0x0A, 0x04],
        'W' => [0x11, 0x11, 0x11, 0x15, 0x15, 0x1B, 0x11],
        'X' => [0x11, 0x11, 0x0A, 0x04, 0x0A, 0x11, 0x11],
        'Y' => [0x11, 0x11, 0x0A, 0x04, 0x04, 0x04, 0x04],
        'Z' => [0x1F, 0x01, 0x02, 0x04, 0x08, 0x10, 0x1F],
        'a' => [0x00, 0x00, 0x0E, 0x01, 0x0F, 0x11, 0x0F],
        'b' => [0x10, 0x10, 0x1E, 0x11, 0x11, 0x11, 0x1E],
        'c' => [0x00, 0x00, 0x0E, 0x11, 0x10, 0x11, 0x0E],
        'd' => [0x01, 0x01, 0x0F, 0x11, 0x11, 0x11, 0x0F],
        'e' => [0x00, 0x00, 0x0E, 0x11, 0x1F, 0x10, 0x0E],
        'f' => [0x06, 0x08, 0x1C, 0x08, 0x08, 0x08, 0x08],
        'g' => [0x00, 0x00, 0x0F, 0x11, 0x0F, 0x01, 0x0E],
        'h' => [0x10, 0x10, 0x1E, 0x11, 0x11, 0x11, 0x11],
        'i' => [0x04, 0x00, 0x0C, 0x04, 0x04, 0x04, 0x0E],
        'j' => [0x02, 0x00, 0x06, 0x02, 0x02, 0x12, 0x0C],
        'k' => [0x10, 0x10, 0x12, 0x14, 0x18, 0x14, 0x12],
        'l' => [0x0C, 0x04, 0x04, 0x04, 0x04, 0x04, 0x0E],
        'm' => [0x00, 0x00, 0x1A, 0x15, 0x15, 0x15, 0x15],
        'n' => [0x00, 0x00, 0x1E, 0x11, 0x11, 0x11, 0x11],
        'o' => [0x00, 0x00, 0x0E, 0x11, 0x11, 0x11, 0x0E],
        'p' => [0x00, 0x00, 0x1E, 0x11, 0x1E, 0x10, 0x10],
        'q' => [0x00, 0x00, 0x0F, 0x11, 0x0F, 0x01, 0x01],
        'r' => [0x00, 0x00, 0x16, 0x19, 0x10, 0x10, 0x10],
        's' => [0x00, 0x00, 0x0F, 0x10, 0x0E, 0x01, 0x1E],
        't' => [0x08, 0x08, 0x1C, 0x08, 0x08, 0x09, 0x06],
        'u' => [0x00, 0x00, 0x11, 0x11, 0x11, 0x13, 0x0D],
        'v' => [0x00, 0x00, 0x11, 0x11, 0x11, 0x0A, 0x04],
        'w' => [0x00, 0x00, 0x11, 0x15, 0x15, 0x15, 0x0A],
        'x' => [0x00, 0x00, 0x11, 0x0A, 0x04, 0x0A, 0x11],
        'y' => [0x00, 0x00, 0x11, 0x11, 0x0F, 0x01, 0x0E],
        'z' => [0x00, 0x00, 0x1F, 0x02, 0x04, 0x08, 0x1F],
        '0' => [0x0E, 0x11, 0x13, 0x15, 0x19, 0x11, 0x0E],
        '1' => [0x04, 0x0C, 0x04, 0x04, 0x04, 0x04, 0x0E],
        '2' => [0x0E, 0x11, 0x01, 0x06, 0x08, 0x10, 0x1F],
        '3' => [0x1F, 0x02, 0x04, 0x02, 0x01, 0x11, 0x0E],
        '4' => [0x02, 0x06, 0x0A, 0x12, 0x1F, 0x02, 0x02],
        '5' => [0x1F, 0x10, 0x1E, 0x01, 0x01, 0x11, 0x0E],
        '6' => [0x06, 0x08, 0x10, 0x1E, 0x11, 0x11, 0x0E],
        '7' => [0x1F, 0x01, 0x02, 0x04, 0x08, 0x08, 0x08],
        '8' => [0x0E, 0x11, 0x11, 0x0E, 0x11, 0x11, 0x0E],
        '9' => [0x0E, 0x11, 0x11, 0x0F, 0x01, 0x02, 0x0C],
        '.' => [0x00, 0x00, 0x00, 0x00, 0x00, 0x0C, 0x0C],
        ',' => [0x00, 0x00, 0x00, 0x00, 0x0C, 0x04, 0x08],
        '-' => [0x00, 0x00, 0x00, 0x0E, 0x00, 0x00, 0x00],
        '/' => [0x01, 0x01, 0x02, 0x04, 0x08, 0x10, 0x10],
        '(' => [0x02, 0x04, 0x08, 0x08, 0x08, 0x04, 0x02],
        ')' => [0x08, 0x04, 0x02, 0x02, 0x02, 0x04, 0x08],
        ':' => [0x00, 0x0C, 0x0C, 0x00, 0x0C, 0x0C, 0x00],
        '+' => [0x00, 0x04, 0x04, 0x1F, 0x04, 0x04, 0x00],
        '%' => [0x19, 0x19, 0x02, 0x04, 0x08, 0x13, 0x13],
        '_' => [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1F],
        _ => [0; 7],
    }
}

fn encode_png(width: usize, height: usize, rgb: &[u8]) -> Vec<u8> {
    let mut raw = Vec::with_capacity(height * (width * 3 + 1));
    for row in rgb.chunks_exact(width * 3) {
        raw.push(0);
        raw.extend_from_slice(row);
    }

    let mut idat = vec![0x78, 0x01];
    let mut offset = 0;
    while offset < raw.len() {
        let n = (raw.len() - offset).min(65_535);
        let last = offset + n == raw.len();
        idat.push(u8::from(last));
        idat.extend_from_slice(&(n as u16).to_le_bytes());
        idat.extend_from_slice(&(!(n as u16)).to_le_bytes());
        idat.extend_from_slice(&raw[offset..offset + n]);
        offset += n;
    }
    idat.extend_from_slice(&adler32(&raw).to_be_bytes());

    let mut ihdr = Vec::new();
    ihdr.extend_from_slice(&(width as u32).to_be_bytes());
    ihdr.extend_from_slice(&(height as u32).to_be_bytes());
    ihdr.extend_from_slice(&[8, 2, 0, 0, 0]);

    let mut png = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    png_chunk(&mut png, b"IHDR", &ihdr);
    png_chunk(&mut png, b"IDAT", &idat);
    png_chunk(&mut png, b"IEND", &[]);
    png
}

fn png_chunk(out: &mut Vec<u8>, kind: &[u8; 4], data: &[u8]) {
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(kind);
    out.extend_from_slice(data);
    let mut crc_input = Vec::with_capacity(4 + data.len());
    crc_input.extend_from_slice(kind);
    crc_input.extend_from_slice(data);
    out.extend_from_slice(&crc32(&crc_input).to_be_bytes());
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for byte in data {
        crc ^= *byte as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

fn adler32(data: &[u8]) -> u32 {
    let mut a = 1u32;
    let mut b = 0u32;
    for byte in data {
        a = (a + *byte as u32) % 65_521;
        b = (b + a) % 65_521;
    }
    (b << 16) | a
}
