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
use std::{collections::HashMap, convert::TryFrom};

/// ORC data TypeKinds
/// They differ from ThinTypes in the sense that List(Int) and List(Long) are of the same typekind,
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

impl TypeKind {
    // Definition of primitiveness:
    // Does ThinType exclusively depend on TypeKind?
    // For example Boolean is primitive while List and Decimal aren't
    pub fn is_primitive(&self) -> bool {
        !matches!(
            self,
            TypeKind::List
                | TypeKind::Map
                | TypeKind::Struct
                | TypeKind::Union
                | TypeKind::Decimal
                | TypeKind::Varchar
                | TypeKind::Char
        )
    }

    // Is char or varchar?
    pub fn is_char(&self) -> bool {
        matches!(self, TypeKind::Char | TypeKind::Varchar)
    }
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

impl From<ThinType> for TypeKind {
    fn from(value: ThinType) -> Self {
        match value {
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

// Only works for primitives
impl TryFrom<TypeKind> for ThinType {
    type Error = OrcError;
    fn try_from(value: TypeKind) -> OrcResult<Self> {
        match value {
            TypeKind::Boolean => Ok(ThinType::Boolean),
            TypeKind::Byte => Ok(ThinType::Byte),
            TypeKind::Short => Ok(ThinType::Short),
            TypeKind::Int => Ok(ThinType::Int),
            TypeKind::Long => Ok(ThinType::Long),
            TypeKind::Float => Ok(ThinType::Float),
            TypeKind::Double => Ok(ThinType::Double),
            TypeKind::String => Ok(ThinType::String),
            TypeKind::Binary => Ok(ThinType::Binary),
            TypeKind::Timestamp => Ok(ThinType::Timestamp),
            TypeKind::Date => Ok(ThinType::Date),
            TypeKind::TimestampInstant => Ok(ThinType::TimestampInstant),
            _ => Err(OrcError::DataTypeError(
                "Can not convert a non-primitive TypeKind to a ThinType".to_string(),
            )),
        }
    }
}

/// ORC data types
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataType {
    parent: Box<Option<DataType>>,
    pub column_id: Option<usize>,
    pub maximum_column_id: Option<usize>,
    pub thin_type: ThinType,
    attributes: HashMap<String, String>,
    pub subtype_count: usize,
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
    pub fn new(thin_type: &ThinType) -> Self {
        DataType {
            parent: Box::new(None),
            column_id: None,
            maximum_column_id: None,
            thin_type: thin_type.clone(),
            attributes: HashMap::new(),
            subtype_count: 0,
        }
    }

    pub fn get_subtype(&self, child_id: usize) -> OrcResult<Box<DataType>> {
        match &(self.thin_type) {
            ThinType::List(s) => match child_id {
                0 => Ok(s.clone()),
                _ => Err(OrcError::DataTypeError(
                    "Lists have only one subtype.".to_string(),
                )),
            },
            ThinType::Map(k, v) => match child_id {
                0 => Ok(k.clone()),
                1 => Ok(v.clone()),
                _ => Err(OrcError::DataTypeError(
                    "Maps have only two subtypes.".to_string(),
                )),
            },
            ThinType::Struct(f) => {
                if child_id < self.subtype_count {
                    Ok(f[child_id].datatype.clone())
                } else {
                    Err(OrcError::DataTypeError("Index out of bound.".to_string()))
                }
            }
            ThinType::Union(s) => {
                if child_id < self.subtype_count {
                    Ok(s[child_id].clone())
                } else {
                    Err(OrcError::DataTypeError("Index out of bound.".to_string()))
                }
            }
            _ => Err(OrcError::DataTypeError(
                "Primitive types do not have subtypes.".to_string(),
            )),
        }
    }

    pub fn get_field_name(&self, child_id: usize) -> OrcResult<String> {
        match &(self.thin_type) {
            ThinType::Struct(f) => {
                if child_id < self.subtype_count {
                    Ok(f[child_id].name.clone())
                } else {
                    Err(OrcError::DataTypeError("Index out of bound.".to_string()))
                }
            }
            _ => Err(OrcError::DataTypeError(
                "Non-structs do not have fieldnames.".to_string(),
            )),
        }
    }

    pub fn get_maximum_length(&self) -> OrcResult<u64> {
        match &(self.thin_type) {
            ThinType::Char(max_length) | ThinType::Varchar(max_length) => Ok(*max_length),
            _ => Err(OrcError::DataTypeError(
                "DataTypes other than Char or Varchar do not have maximum length.".to_string(),
            )),
        }
    }

    pub fn get_precision(&self) -> OrcResult<u64> {
        match &(self.thin_type) {
            ThinType::Decimal(precision, _) => Ok(*precision),
            _ => Err(OrcError::DataTypeError(
                "DataTypes other than Decimal do not have precision.".to_string(),
            )),
        }
    }

    pub fn get_scale(&self) -> OrcResult<u64> {
        match &(self.thin_type) {
            ThinType::Decimal(_, scale) => Ok(*scale),
            _ => Err(OrcError::DataTypeError(
                "DataTypes other than Decimal do not have scale.".to_string(),
            )),
        }
    }
}

/// Create numerous DataTypes
pub fn create_primitive_type(kind: &TypeKind) -> OrcResult<Box<DataType>> {
    if kind.is_primitive() {
        Ok(Box::new(DataType::new(&ThinType::try_from(*kind)?)))
    } else {
        Err(OrcError::DataTypeError(
            "The TypeKind is not primitive".to_string(),
        ))
    }
}

pub fn create_char_type(kind: &TypeKind, max_length: u64) -> OrcResult<Box<DataType>> {
    let thin_type_result = match kind {
        TypeKind::Char => Ok(ThinType::Char(max_length)),
        TypeKind::Varchar => Ok(ThinType::Varchar(max_length)),
        _ => Err(OrcError::DataTypeError(
            "The TypeKind is not Char or Varchar".to_string(),
        )),
    };
    Ok(Box::new(DataType::new(&thin_type_result?)))
}

pub fn create_decimal_type(
    kind: &TypeKind,
    precision: u64,
    scale: u64,
) -> OrcResult<Box<DataType>> {
    match kind {
        TypeKind::Decimal => {
            let thin_type = ThinType::Decimal(precision, scale);
            Ok(Box::new(DataType::new(&thin_type)))
        }
        _ => Err(OrcError::DataTypeError(
            "The TypeKind is not decimal".to_string(),
        )),
    }
}

// Create new struct type with no fields
pub fn create_struct_type() -> OrcResult<Box<DataType>> {
    let thin_type = ThinType::Struct(Vec::new());
    Ok(Box::new(DataType::new(&thin_type)))
}

pub fn create_list_type(element_type: &Box<DataType>) -> OrcResult<Box<DataType>> {
    let thin_type = ThinType::List(element_type.clone());
    Ok(Box::new(DataType::new(&thin_type)))
}

pub fn create_map_type(
    key_type: &Box<DataType>,
    value_type: &Box<DataType>,
) -> OrcResult<Box<DataType>> {
    let thin_type = ThinType::Map(key_type.clone(), value_type.clone());
    Ok(Box::new(DataType::new(&thin_type)))
}

// Create new union type with no fields
pub fn create_union_type() -> OrcResult<Box<DataType>> {
    let thin_type = ThinType::Union(Vec::new());
    Ok(Box::new(DataType::new(&thin_type)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_kind_primitive() {
        let type_kind = TypeKind::Boolean;
        assert!(type_kind.is_primitive());

        let type_kind = TypeKind::List;
        assert!(!type_kind.is_primitive());
    }

    #[test]
    fn test_type_kind_char() {
        let type_kind = TypeKind::Char;
        assert!(type_kind.is_char());

        let type_kind = TypeKind::List;
        assert!(!type_kind.is_char());
    }

    #[test]
    fn test_type_kind_from_thin_type() {
        let thin_type = ThinType::Boolean;
        assert_eq!(TypeKind::from(thin_type), TypeKind::Boolean);

        let thin_type = ThinType::Decimal(26, 6);
        assert_eq!(TypeKind::from(thin_type), TypeKind::Decimal);

        let entry_datatype = create_primitive_type(&TypeKind::Long).unwrap();
        let list_thin_type = ThinType::List(entry_datatype);
        assert_eq!(TypeKind::from(list_thin_type), TypeKind::List);
    }

    #[test]
    fn test_thin_type_from_type_kind() {
        let type_kind = TypeKind::Boolean;
        assert_eq!(ThinType::try_from(type_kind).unwrap(), ThinType::Boolean);
    }

    #[test]
    #[should_panic(expected = "Can not convert a non-primitive TypeKind to a ThinType")]
    fn test_thin_type_from_type_kind_panic() {
        let type_kind = TypeKind::Struct;
        ThinType::try_from(type_kind).unwrap();
    }
}
