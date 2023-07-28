use std::{
    fs::File,
    io::{self, BufReader},
    time::{Instant, Duration}, collections::HashMap,
};
use itertools::Itertools;
use num_enum::IntoPrimitive;
use num_enum::TryFromPrimitive;
use anyhow::{anyhow, Ok};
use chrono::TimeZone;
use chrono::Timelike;
use chrono::Utc;
use csv::{ReaderBuilder, StringRecord};
use flate2::read::GzDecoder;
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use sqlx::{sqlite::SqlitePoolOptions, Pool, Sqlite};

#[tokio::main]
async fn main() {
    run().await.unwrap();
}

async fn run() -> anyhow::Result<()> {
    let entries = std::fs::read_dir("..")?;

    std::fs::File::create("db.sqlite")?;

    let pool = SqlitePoolOptions::new()
    .max_connections(5)
    .acquire_timeout(Duration::from_secs(3))
    .connect("sqlite://db.sqlite")
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS pixels (
            timestamp INTEGER,
            id INTEGER,
            x INTEGER,
            y INTEGER,
            x1 INTEGER,
            y1 INTEGER,
            color INTEGER
        )
        "#
    )
    .execute(&pool)
    .await?;

    let file_paths: Vec<String> = entries
        .filter_map(|e| {
            match e {
                anyhow::Result::Ok(entry) => {
                    if let Some(file_name) = entry.file_name().to_str() {
                        if file_name.ends_with(".csv.gzip") {
                            return Some(file_name.to_string());
                        }
                    }
                }
                _ => (),
            }
            None
        })
        .map(|a| format!("../{}", a))
        .collect();

    dbg!(&file_paths);

    let mut map: HashMap<String, i64> = HashMap::with_capacity(500000);
    let mut count = 0i64;
    let mut fc = 0;

    for f in file_paths.iter() {
        println!("file: {}", fc);
        read_csv_gzip_file(f, &mut map, &mut count, &pool).await?;
        fc+= 1;
    }

    Ok(())
}

async fn read_csv_gzip_file(file_path: &str, map: &mut HashMap<String, i64>, count: &mut i64, pool: &Pool<Sqlite>) -> anyhow::Result<()> {
    let file = File::open(file_path)?;
    let gz_decoder = GzDecoder::new(file);
    let reader = BufReader::new(gz_decoder);
    let mut csv_reader = ReaderBuilder::new().from_reader(reader);
    dbg!(csv_reader.headers()?);

    let mut chunks = csv_reader.records().chunks(100_000);

    for chunk in  chunks.into_iter() {
        let mut transaction = pool.begin().await?;
        for record in chunk {
            let r = record?;
            let pix = read_record(r, map, count)?;
            let q = sqlx::query("INSERT INTO pixels (timestamp, id, x, y, x0, x1, color) VALUES ($1, $2, $3, $4, $5, $6, $7)")
            .bind(pix.timestamp)
            .bind(pix.id)
            .bind(pix.x)
            .bind(pix.y)
            .bind(pix.x1)
            .bind(pix.y1)
            .bind(pix.color)
            .execute(&mut *transaction).await;
        }
        transaction.commit().await?;
        println!("transaction");
    }

    Ok(())
}

fn read_record(record: StringRecord, map: &mut HashMap<String, i64>, count: &mut i64) -> anyhow::Result<RedditPixel> {
    let a = record.get(0).ok_or(anyhow!("bad format"))?;
    let timestamp = date_components_to_timestamp(read_date(a)?)?;

    let string_id = record.get(1).ok_or(anyhow!("bad format"))?;
    let id;
    if let Some(e) = map.get(string_id) {
        id = *e;
    } else {
        *count += 1;
        id = *count;
        map.insert(string_id.to_string(), id);
    }

    let b = record.get(2).ok_or(anyhow!("bad format"))?;
    let coords = read_coords(b)?;

    let c = record.get(3).ok_or(anyhow!("bad format"))?;
    let color = read_color(c)?;



    Ok(RedditPixel {
        timestamp,
        id,
        x: coords.0,
        y: coords.1,
        x1: coords.2,
        y1: coords.3,
        color
    })
}

fn read_date(text: &str) -> anyhow::Result<[u32; 7]> {
    let mut components = [0u32; 7];
    let mut current = 0usize;
    let mut multiplier = 10000u32;
    for c in text.chars() {
        if c.is_ascii_digit() {
            components[current] += c.to_digit(10).ok_or(anyhow!("bad digit"))? * multiplier;
            multiplier /= 10;
        } else {
            if current >= components.len() {
                return Ok(components);
            }
            components[current] /= (multiplier * 10);
            current += 1;
            multiplier = 1000;
        }
    }

    Err(anyhow!("bad date"))
}

fn date_components_to_timestamp(c: [u32; 7]) -> anyhow::Result<i64> {
    let dt = Utc
        .with_ymd_and_hms(c[0].try_into()?, c[1], c[2], c[3], c[4], c[5])
        .earliest()
        .ok_or(anyhow!(""))?;

    Ok(dt.timestamp_millis() + (c[6] as i64))
}

fn read_coords(text: &str) -> anyhow::Result<(i32, i32, i32, i32)> {
    let mut comma_indices = Vec::with_capacity(4);

    for (index, character) in text.chars().enumerate() {
        if character == ',' {
            comma_indices.push(index);
        }
    }

    match comma_indices.len() {
        1 => {
            let first = &text[0..comma_indices[0]];
            let second = &text[comma_indices[0]+1..];
            Ok((i32::from_str_radix(first, 10)?, i32::from_str_radix(second, 10)?, i32::MAX, i32::MAX))
        }
        2 => {
            // these idiots put JSON in my CSV ffs
            // turns out it's not even JSON
            let mut colon_indices = Vec::with_capacity(4);
            for (index, character) in text.chars().enumerate() {
                if character == ':' {
                    colon_indices.push(index);
                }
            }
            let first = &text[colon_indices[0]+2..comma_indices[0]];
            let second = &text[colon_indices[1]+2..comma_indices[1]];
            let third = &text[colon_indices[2]+2..text.len()-1];
            //println!("Mod circle");
            Ok((i32::from_str_radix(first, 10)?, i32::from_str_radix(second, 10)?, i32::from_str_radix(third, 10)?, i32::MAX))
        }
        3 => {
            let first = &text[0..comma_indices[0]];
            let second = &text[comma_indices[0]+1..comma_indices[1]];
            let third = &text[comma_indices[1]+1..comma_indices[2]];
            let fourth = &text[comma_indices[2]+1..];
            //println!("Mod rectangle");
            Ok((i32::from_str_radix(first, 10)?, i32::from_str_radix(second, 10)?, i32::from_str_radix(third, 10)?, i32::from_str_radix(fourth, 10)?))
        }
        _ => Err(anyhow!("formatting error"))
    }
}

fn read_color(text: &str) -> anyhow::Result<u32> {
    Ok(u32::from_str_radix(&text[1..], 16)?)

/*
    // set alpha
    let color = Colors::try_from_primitive((0xFF << 24) | color_value)?;
    Ok(get_color_id(color)) */
}

#[derive(Debug, Clone, Copy, sqlx::FromRow)]
struct RedditPixel {
    timestamp: i64,
    id: i64,
    x: i32,
    y: i32,
    x1: i32,
    y1: i32,
    color: u32
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[repr(u32)]
enum Colors
{
    Burgundy = 4279894125,
    DarkRed = 4281925822,
    Red = 4278207999,
    Orange = 4278233343,
    Yellow = 4281718527,
    PaleYellow = 4290312447,
    DarkGreen = 4285047552,
    Green = 4286106624,
    LightGreen = 4283886974,
    DarkTeal = 4285494528,
    Teal = 4289371648,
    LightTeal = 4290825216,
    DarkBlue = 4288958500,
    Blue = 4293562422,
    LightBlue = 4294240593,
    Indigo = 4290853449,
    Periwinkle = 4294925418,
    Lavender = 4294947732,
    DarkPurple = 4288618113,
    Purple = 4290792116,
    PalePurple = 4294945764,
    Magenta = 4286517470,
    Pink = 4286684415,
    LightPink = 4289370623,
    DarkBrown = 4281288813,
    Brown = 4280707484,
    Beige = 4285576447,
    Black = 4278190080,
    DarkGray = 4283585105,
    Gray = 4287663497,
    LightGray = 4292466644,
    White = 4294967295
}

fn get_color_id(c: Colors) -> u8
{
    match c {
        Colors::Burgundy => 0,
        Colors::DarkRed => 1,
        Colors::Red => 2,
        Colors::Orange => 3,
        Colors::Yellow => 4,
        Colors::PaleYellow => 5,
        Colors::DarkGreen => 6,
        Colors::Green => 7,
        Colors::LightGreen => 8,
        Colors::DarkTeal => 9,
        Colors::Teal => 10,
        Colors::LightTeal => 11,
        Colors::DarkBlue => 12,
        Colors::Blue => 13,
        Colors::LightBlue => 14,
        Colors::Indigo => 15,
        Colors::Periwinkle => 16,
        Colors::Lavender => 17,
        Colors::DarkPurple => 18,
        Colors::Purple => 19,
        Colors::PalePurple => 20,
        Colors::Magenta => 21,
        Colors::Pink => 22,
        Colors::LightPink => 23,
        Colors::DarkBrown => 24,
        Colors::Brown => 25,
        Colors::Beige => 26,
        Colors::Black => 27,
        Colors::DarkGray => 28,
        Colors::Gray => 29,
        Colors::LightGray => 30,
        Colors::White => 31
    }
}