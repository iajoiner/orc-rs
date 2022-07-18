// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::error::{OrcError, OrcResult};
use std::collections::HashMap;

/// ORC data typekinds
/// They differ from datatypes in the sense that List(Int) and List(Long) are of the same typekind,
/// List while having different types.
///
/// See https://orc.apache.org/specification/ORCv1/ for more details.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TypeKind {
    Boolean = 0,
    Byte = 1,
    Short = 2,
    Int = 3,
    Long = 4,
    Float = 5,
    Double = 6,
    String = 7,
    Binary = 8,
    Timestamp = 9,
    List = 10,
    Map = 11,
    Struct = 12,
    Union = 13,
    Decimal = 14,
    Date = 15,
    Varchar = 16,
    Char = 17,
    TimestampInstant = 18,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThinType {
    Boolean,
    Byte,
    Short,
    Int,
    Long,
    Float,
    Double,
    String,
    Binary,
    Timestamp,
    List(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
    Struct(Vec<Box<Field>>),
    Union(Vec<Box<DataType>>),
    Decimal(u64, u64),
    Date,
    Varchar(u64),
    Char(u64),
    TimestampInstant,
}

impl ThinType {
    pub fn get_kind(&self) -> TypeKind {
        match self {
            ThinType::Boolean => TypeKind::Boolean,
            ThinType::Byte => TypeKind::Byte,
            ThinType::Short => TypeKind::Short,
            ThinType::Int => TypeKind::Int,
            ThinType::Long => TypeKind::Long,
            ThinType::Float => TypeKind::Float,
            ThinType::Double => TypeKind::Double,
            ThinType::String => TypeKind::String,
            ThinType::Binary => TypeKind::Binary,
            ThinType::Timestamp => TypeKind::Timestamp,
            ThinType::List(_) => TypeKind::List,
            ThinType::Map(_, _) => TypeKind::Map,
            ThinType::Struct(_) => TypeKind::Struct,
            ThinType::Union(_) => TypeKind::Union,
            ThinType::Decimal(_, _) => TypeKind::Decimal,
            ThinType::Date => TypeKind::Date,
            ThinType::Varchar(_) => TypeKind::Varchar,
            ThinType::Char(_) => TypeKind::Char,
            ThinType::TimestampInstant => TypeKind::TimestampInstant,
        }
    }
}

/// ORC data types
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataType {
    parent: Box<DataType>,
    pub column_id: usize,
    pub maximum_column_id: usize,
    pub thin_type: ThinType,
    attributes: HashMap<String, String>,
    pub subtype_count: usize
}

/// ORC fields
///
/// A field is a data DataType which has a name
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub datatype: Box<DataType>,
}

impl DataType {
    pub fn get_subtype(&self, child_id: usize) -> OrcResult<Box<DataType>> {
        match &(self.thin_type) {
            ThinType::List(s) => {
                match child_id {
                    0 => Ok(s.clone()),
                    _ => Err(OrcError::DataTypeError(
                        "Lists have only one subtype.".to_string()
                    ))
                }
            },
            ThinType::Map(k, v) => {
                match child_id {
                    0 => Ok(k.clone()),
                    1 => Ok(v.clone()),
                    _ => Err(OrcError::DataTypeError(
                        "Maps have only two subtypes.".to_string()
                    ))
                }
            },
            ThinType::Struct(f) => {
                if child_id < self.subtype_count {
                    Ok(f[child_id].datatype.clone())
                }
                else {
                    Err(OrcError::DataTypeError("Index out of bound.".to_string()))
                }
            },
            ThinType::Union(s) => {
                if child_id < self.subtype_count {
                    Ok(s[child_id].clone())
                }
                else {
                    Err(OrcError::DataTypeError("Index out of bound.".to_string()))
                }
            },
            _ => Err(OrcError::DataTypeError(
                "Primitive types do not have subtypes.".to_string()
            )),
        }
    }

    pub fn get_field_name(&self, child_id: usize) -> OrcResult<String> {
        match &(self.thin_type) {
            ThinType::Struct(f) => {
                if child_id < self.subtype_count {
                    Ok(f[child_id].name.clone())
                }
                else {
                    Err(OrcError::DataTypeError("Index out of bound.".to_string()))
                }
            },
            _ => Err(OrcError::DataTypeError(
                "Non-structs do not have fieldnames.".to_string()
            )),
        }
    }

    pub fn get_maximum_length(&self) -> OrcResult<u64> {
        match &(self.thin_type) {
            ThinType::Char(max_length) | ThinType::Varchar(max_length) => Ok(*max_length),
            _ => Err(OrcError::DataTypeError(
                "DataTypes other than Char or Varchar do not have maximum length.".to_string()
            )),
        }
    }

    pub fn get_precision(&self) -> OrcResult<u64> {
        match &(self.thin_type) {
            ThinType::Decimal(precision, _) => Ok(*precision),
            _ => Err(OrcError::DataTypeError(
                "DataTypes other than Decimal do not have precision.".to_string()
            )),
        }
    }

    pub fn get_scale(&self) -> OrcResult<u64> {
        match &(self.thin_type) {
            ThinType::Decimal(_, scale) => Ok(*scale),
            _ => Err(OrcError::DataTypeError(
                "DataTypes other than Decimal do not have scale.".to_string()
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_() {

    }
}
