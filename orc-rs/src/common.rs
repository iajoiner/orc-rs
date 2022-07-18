//! Includes all the shared enum for the entire project
pub struct FileVersion {
    pub major_version: u32,
    pub minor_version: u32,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriterId {
    OrcJavaWriter = 0,
    OrcCppWriter = 1,
    PrestoWriter = 2,
    OrcRustWriter = 3,
    UnknownWriter = u32::MAX,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompressionKind {
    None = 0,
    Zlib = 1,
    Snappy = 2,
    Lz0 = 3,
    Lz4 = 4,
    Zstd = 5,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriterVersion {
    Original = 0,
    Hive8732 = 1,
    Hive4243 = 2,
    Hive12055 = 3,
    Hive13083 = 4,
    Orc101 = 5,
    Orc135 = 6,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamKind {
    Present = 0,
    Data = 1,
    Length = 2,
    DictionaryData = 3,
    DictionaryCount = 4,
    Secondary = 5,
    RowIndex = 6,
    BloomFilter = 7,
    BloomFilterUtf8 = 8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnEncodingKind {
    Direct = 0,
    Dictionary = 1,
    DirectV2 = 2,
    DictionaryV2 = 3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BloomFilterVersion {
    // Include both the BloomFilter and BloomFilterUtf8 streams to support
    // both old and new readers.
    Original = 0,
    // Only include the BloomFilterUtf8 streams that consistently use UTF8.
    // See ORC-101
    Utf8 = 1,
}
