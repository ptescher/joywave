use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, Decimal256Array, StringArray, UInt64Array,
};
use datafusion::arrow::datatypes::i256;
use primitive_types::U256;
use std::convert::TryFrom;
use std::sync::Arc;

use datafusion::prelude::*;
use datafusion::{arrow::datatypes::DataType, logical_expr::Volatility};
use datafusion_expr::{ColumnarValue, ScalarUDF};

fn parse_uint64_bytes(item: &[u8]) -> Result<Option<u64>, datafusion::error::DataFusionError> {
    if item.len() == 32 {
        let (_padding, data) = item.split_at(16);
        let value = u128::from_be_bytes(<[u8; 16]>::try_from(data).unwrap());
        Ok(Some(value as u64))
    } else if item.len() == 16 {
        let value = u128::from_be_bytes(<[u8; 16]>::try_from(item).unwrap());
        Ok(Some(value as u64))
    } else if item.len() == 8 {
        let value: u64 = u64::from_be_bytes(<[u8; 8]>::try_from(item).unwrap());
        Ok(Some(value))
    } else if item.is_empty() {
        Ok(None)
    } else {
        Err(datafusion::error::DataFusionError::Internal(format!(
            "Expected 8 or 16 byte array, not {}",
            item.len()
        )))
    }
}

pub fn uint64_from_bytes() -> ScalarUDF {
    // First, declare the actual implementation of the calculation
    let decimal_from_bytes = Arc::new(|args: &[ColumnarValue]| {
        if let ColumnarValue::Array(array) = &args[0] {
            let binary_array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let mut values = Vec::with_capacity(binary_array.len());

            for item in binary_array {
                if let Some(item) = item {
                    values.push(parse_uint64_bytes(item)?);
                } else {
                    values.push(None);
                }
            }

            let array = UInt64Array::from(values);
            Ok(ColumnarValue::Array(Arc::new(array) as ArrayRef))
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "Expected binary array".to_string(),
            ))
        }
    });

    create_udf(
        "uint64_from_bytes",
        vec![DataType::Binary],
        DataType::UInt64,
        Volatility::Immutable,
        decimal_from_bytes,
    )
}

fn parse_i256_bytes(item: &[u8]) -> Result<Option<i256>, datafusion::error::DataFusionError> {
    if item.len() == 32 {
        let number = i256::from_be_bytes(<[u8; 32]>::try_from(item).unwrap());
        Ok(Some(number))
    } else if item.len() == 16 {
        let value = u128::from_be_bytes(<[u8; 16]>::try_from(item).unwrap());
        Ok(Some(i256::from_i128(value as i128)))
    } else if item.len() == 8 {
        let value: u64 = u64::from_be_bytes(<[u8; 8]>::try_from(item).unwrap());
        Ok(Some(i256::from_i128(value as i128)))
    } else if item.is_empty() {
        Ok(None)
    } else {
        Err(datafusion::error::DataFusionError::Internal(format!(
            "Expected 8 or 16 byte array, not {}",
            item.len()
        )))
    }
}

pub fn decimal256_from_bytes() -> ScalarUDF {
    // First, declare the actual implementation of the calculation
    let decimal_from_bytes = Arc::new(|args: &[ColumnarValue]| {
        if let ColumnarValue::Array(array) = &args[0] {
            let binary_array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let mut values = Vec::with_capacity(binary_array.len());

            for item in binary_array {
                if let Some(item) = item {
                    values.push(parse_i256_bytes(item)?);
                } else {
                    values.push(None);
                }
            }

            let array = Decimal256Array::from(values);
            Ok(ColumnarValue::Array(Arc::new(array) as ArrayRef))
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "Expected binary array".to_string(),
            ))
        }
    });

    create_udf(
        "decimal256_from_bytes",
        vec![DataType::Binary],
        DataType::Decimal256(36, 0),
        Volatility::Immutable,
        decimal_from_bytes,
    )
}

pub fn string_from_uint256_bytes() -> ScalarUDF {
    // First, declare the actual implementation of the calculation
    let decimal_from_bytes = Arc::new(|args: &[ColumnarValue]| {
        if let ColumnarValue::Array(array) = &args[0] {
            let binary_array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let mut values = Vec::with_capacity(binary_array.len());

            for item in binary_array {
                if let Some(item) = item {
                    if item.len() > 32 {
                        log::warn!("Got bytes that are {} bytes long: {:?}", item.len(), item);
                        values.push(None);
                    } else {
                        let uint = U256::from_big_endian(item);
                        values.push(Some(uint.to_string()));
                    }
                } else {
                    values.push(None);
                }
            }

            let array = StringArray::from(values);
            Ok(ColumnarValue::Array(Arc::new(array) as ArrayRef))
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "Expected binary array".to_string(),
            ))
        }
    });

    create_udf(
        "string_from_uint256_bytes",
        vec![DataType::Binary],
        DataType::Utf8,
        Volatility::Immutable,
        decimal_from_bytes,
    )
}

pub fn b58_string_from_bytes() -> ScalarUDF {
    // First, declare the actual implementation of the calculation
    let b58_string_from_bytes = Arc::new(|args: &[ColumnarValue]| {
        if let ColumnarValue::Array(array) = &args[0] {
            let binary_array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let mut values = Vec::with_capacity(binary_array.len());

            for item in binary_array {
                if let Some(item) = item {
                    values.push(Some(bs58::encode(item).into_string()));
                } else {
                    values.push(None);
                }
            }

            let array = StringArray::from(values);
            Ok(ColumnarValue::Array(Arc::new(array) as ArrayRef))
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "Expected binary array".to_string(),
            ))
        }
    });

    create_udf(
        "b58_string_from_bytes",
        vec![DataType::Binary],
        DataType::Utf8,
        Volatility::Immutable,
        b58_string_from_bytes,
    )
}
