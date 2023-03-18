use std::path::{Path, PathBuf};
use structopt::StructOpt;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt};
use deltalake::action::*;
use deltalake::arrow::array::*;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::*;
use log::*;
use std::collections::HashMap;
use std::io::BufRead;
use std::sync::Arc;
use chrono::DateTime;
use deltalake::arrow::datatypes::DataType;
use tokio::io;

#[derive(Debug, StructOpt)]
#[structopt(name = "csv-to-delta", about = "CLI tool to write CSV to Delta table")]
struct Opt {
  #[structopt(short, long)]
  input_file: PathBuf,

  #[structopt(short, long)]
  table_uri: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
  let opt = Opt::from_args();
  let input_file: PathBuf = opt.input_file;
  let table_uri: String = opt.table_uri;

  info!("Using the location of: {:?}", table_uri);
  let table_path = Path::new(&table_uri);

  // Load the input file and build a schema
  let file = File::open(input_file).await?;
  let reader = io::BufReader::new(file);

  let mut lines = reader.lines();
  let header_line = lines.next_line().await?.unwrap();
  let headers: Vec<&str> = header_line.split(',').collect();

  // For now, only accept string values.
  let schema_fields: Vec<SchemaField> = headers.iter().map(|h|
      SchemaField::new(h.to_string(), SchemaDataType::primitive("string".to_string()), true, HashMap::new())
  ).collect();

  let schema = Schema::new(schema_fields);

  // Load the delta table or create a new one with the schema
  let maybe_table = deltalake::open_table(&table_uri).await;
  let mut table = match maybe_table {
    Ok(table) => table,
    Err(DeltaTableError::NotATable(_)) => {
      info!("It doesn't look like our delta table has been created");
      create_initialized_table(&table_uri, schema).await
    }
    Err(err) => Err(err).unwrap(),
  };

  let mut writer =
    RecordBatchWriter::for_table(&table).expect("Failed to make RecordBatchWriter");

  let mut records = vec![];

  while let Some(line) = lines.next_line().await? {
    let values: Vec<&str> = line.split(',').collect();
    let mut record = HashMap::new();
    for (i, value) in values.iter().enumerate() {
      record.insert(headers[i], value.to_string());
    }
    records.push(record);
  }

  let batch = convert_to_batch(&table, &records);

  writer.write(batch).await?;

  let adds = writer
    .flush_and_commit(&mut table)
    .await
    .expect("Failed to flush write");
  info!("{} adds written", adds);

  Ok(())
}

fn convert_to_batch(table: &DeltaTable, records: &[HashMap<&str, String>]) -> RecordBatch {
  let metadata = table
    .get_metadata()
    .expect("Failed to get metadata for the table");
  let arrow_schema = <deltalake::arrow::datatypes::Schema as TryFrom<&Schema>>::try_from(
    &metadata.schema.clone(),
  )
    .expect("Failed to convert to arrow schema");
  let arrow_schema_ref = Arc::new(arrow_schema);

  let mut arrow_arrays: Vec<Arc<dyn Array>> = vec![];

  for field in arrow_schema_ref.fields() {
    let name = field.name();
    let mut values: Vec<String> = vec![];
    for record in records {
      let value_str = record.get(String::from(name).as_str()).unwrap();
      let value = match field.data_type() {
      // TODO: Support other data types
        // DataType::Boolean => value_str.parse::<bool>().unwrap(),
        // DataType::Int8 => value_str.parse::<i8>().unwrap(),
        // DataType::Int16 => value_str.parse::<i16>().unwrap(),
        // DataType::Int32 => value_str.parse::<i32>().unwrap(),
        // DataType::Int64 => value_str.parse::<i64>().unwrap(),
        // DataType::UInt8 => value_str.parse::<u8>().unwrap(),
        // DataType::UInt16 => value_str.parse::<u16>().unwrap(),
        // DataType::UInt32 => value_str.parse::<u32>().unwrap(),
        // DataType::UInt64 => value_str.parse::<u64>().unwrap(),
        // DataType::Float32 => value_str.parse::<f32>().unwrap(),
        // DataType::Float64 => value_str.parse::<f64>().unwrap(),
        DataType::Utf8 => value_str.to_string(),
        // DataType::Timestamp(_, ..) => {
        //   let timestamp = DateTime::parse_from_rfc3339(value_str).unwrap();
        //   timestamp.timestamp_millis()
        // }
        _ => panic!("Unsupported data type: {:?}", field.data_type()),
      };
      values.push(value);
    }
    let arrow_array: Arc<dyn Array> = match field.data_type() {
      // DataType::Boolean => Arc::new(BooleanArray::from(values)),
      // DataType::Int8 => Arc::new(Int8Array::from(values)),
      // DataType::Int16 => Arc::new(Int16Array::from(values)),
      // DataType::Int32 => Arc::new(Int32Array::from(values)),
      // DataType::Int64 => Arc::new(Int64Array::from(values)),
      // DataType::UInt8 => Arc::new(UInt8Array::from(values)),
      // DataType::UInt16 => Arc::new(UInt16Array::from(values)),
      // DataType::UInt32 => Arc::new(UInt32Array::from(values)),
      // DataType::UInt64 => Arc::new(UInt64Array::from(values)),
      // DataType::Float32 => Arc::new(Float32Array::from(values)),
      // DataType::Float64 => Arc::new(Float64Array::from(values)),
      DataType::Utf8 => {
        let utf8_values = values.to_vec();
        Arc::new(StringArray::from(utf8_values))
      }
      // DataType::Timestamp(_, ..) => {
      //   Arc::new(TimestampMillisecondArray::from(values))
      // }
      _ => panic!("Unsupported data type: {:?}", field.data_type()),
    };
    arrow_arrays.push(arrow_array);
  }

  RecordBatch::try_new(arrow_schema_ref, arrow_arrays).expect("Failed to create RecordBatch")
}

async fn create_initialized_table(table_path: &str, table_schema: Schema) -> DeltaTable {
  let mut table = DeltaTableBuilder::from_uri(table_path).build().unwrap();

  let mut commit_info = serde_json::Map::<String, serde_json::Value>::new();
  commit_info.insert(
    "operation".to_string(),
    serde_json::Value::String("CREATE TABLE".to_string()),
  );
  commit_info.insert(
    "userName".to_string(),
    serde_json::Value::String("test user".to_string()),
  );

  let protocol = Protocol {
    min_reader_version: 1,
    min_writer_version: 1,
  };

  let metadata = DeltaTableMetaData::new(None, None, None, table_schema, vec![], HashMap::new());

  table
    .create(metadata, protocol, Some(commit_info), None)
    .await
    .unwrap();

  table
}
