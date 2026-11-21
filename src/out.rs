//! Parquet output: plain encoding, no compression, one row group per file.

use crate::parse::decode;
use crate::URow;
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::sync::Arc;

pub struct CampRow {
    pub camp: usize,
    pub day: i32,
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assisted: i64,
}

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_max_row_group_size(usize::MAX)
            .set_data_page_size_limit(1 << 30)
            .set_write_batch_size(1 << 30)
            .build(),
    )
}

enum Col {
    Bytes(Vec<ByteArray>, Option<Vec<i16>>),
    I32(Vec<i32>),
    I64(Vec<i64>),
}

fn write(path: &std::path::Path, schema: &str, cols: Vec<Col>) {
    let schema = Arc::new(parse_message_type(schema).expect("schema"));
    let file = std::fs::File::create(path).expect("create parquet");
    let buf = std::io::BufWriter::with_capacity(1 << 22, file);
    let mut w = SerializedFileWriter::new(buf, schema, props()).expect("writer");
    let mut rg = w.next_row_group().expect("row group");
    let mut it = cols.into_iter();
    while let Some(mut col) = rg.next_column().expect("column") {
        match it.next().expect("column data") {
            Col::Bytes(v, def) => {
                col.typed::<ByteArrayType>().write_batch(&v, def.as_deref(), None).expect("write");
            }
            Col::I32(v) => {
                col.typed::<Int32Type>().write_batch(&v, None, None).expect("write");
            }
            Col::I64(v) => {
                col.typed::<Int64Type>().write_batch(&v, None, None).expect("write");
            }
        }
        col.close().expect("close column");
    }
    rg.close().expect("close row group");
    w.close().expect("close file");
}

pub fn write_users(path: &std::path::Path, rows: &[URow], cat_names: &[Vec<u8>]) {
    let n = rows.len();
    let mut uid = Vec::with_capacity(n);
    let mut day = Vec::with_capacity(n);
    let mut ses = Vec::with_capacity(n);
    let mut evs = Vec::with_capacity(n);
    let mut rev = Vec::with_capacity(n);
    let mut top = Vec::with_capacity(n);
    let mut def = Vec::with_capacity(n);
    let cats: Vec<ByteArray> = cat_names.iter().map(|c| ByteArray::from(c.clone())).collect();
    let mut b = Vec::with_capacity(16);
    for r in rows {
        b.clear();
        decode(r.person, &mut b);
        uid.push(ByteArray::from(b.clone()));
        day.push(r.day);
        ses.push(r.sessions as i64);
        evs.push(r.events as i64);
        rev.push(r.revenue);
        if r.top == u16::MAX {
            def.push(0);
        } else {
            def.push(1);
            top.push(cats[r.top as usize].clone());
        }
    }
    write(
        path,
        "message user_daily {
            required binary user_id (STRING);
            required int32 day (DATE);
            required int64 sessions;
            required int64 events;
            required int64 revenue_cents;
            optional binary top_category (STRING);
        }",
        vec![
            Col::Bytes(uid, None),
            Col::I32(day),
            Col::I64(ses),
            Col::I64(evs),
            Col::I64(rev),
            Col::Bytes(top, Some(def)),
        ],
    );
}

pub fn write_campaigns(path: &std::path::Path, rows: &[CampRow], camp_ids: &[Vec<u8>], channels: &[Vec<u8>]) {
    let n = rows.len();
    let mut id = Vec::with_capacity(n);
    let mut ch = Vec::with_capacity(n);
    let mut day = Vec::with_capacity(n);
    let mut ses = Vec::with_capacity(n);
    let mut pur = Vec::with_capacity(n);
    let mut rev = Vec::with_capacity(n);
    let mut ass = Vec::with_capacity(n);
    for r in rows {
        if r.camp >= camp_ids.len() {
            id.push(ByteArray::from(&b"organic"[..]));
            ch.push(ByteArray::from(&b"organic"[..]));
        } else {
            id.push(ByteArray::from(camp_ids[r.camp].clone()));
            ch.push(ByteArray::from(channels[r.camp].clone()));
        }
        day.push(r.day);
        ses.push(r.sessions);
        pur.push(r.purchasing);
        rev.push(r.revenue);
        ass.push(r.assisted);
    }
    write(
        path,
        "message campaign_daily {
            required binary campaign_id (STRING);
            required binary channel (STRING);
            required int32 day (DATE);
            required int64 sessions;
            required int64 purchasing_sessions;
            required int64 revenue_cents;
            required int64 assisted_sessions;
        }",
        vec![
            Col::Bytes(id, None),
            Col::Bytes(ch, None),
            Col::I32(day),
            Col::I64(ses),
            Col::I64(pur),
            Col::I64(rev),
            Col::I64(ass),
        ],
    );
}
