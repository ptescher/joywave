use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::arrow::array::AsArray;
use datafusion::error::Result;
use datafusion::{arrow::array::ArrayRef, prelude::*, scalar::ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF, Volatility};
use primitive_types::U256;

#[derive(Debug)]
struct UInt256Sum {
    sum: U256,
}

impl UInt256Sum {
    pub fn new() -> Self {
        UInt256Sum { sum: U256::zero() }
    }
}

impl Accumulator for UInt256Sum {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(
            self.sum.to_big_endian().to_vec(),
        ))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.sum.to_big_endian().to_vec())))
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        if values.len() > 1 {
            panic!("uint256_sum: got {} values, expected 1", values.len());
        }

        let mut uints: Vec<U256> = vec![self.sum];

        let binaries = values[0].as_binary::<i32>();

        for binary in binaries {
            if let Some(binary) = binary {
                if binary.len() > 32 {
                    log::warn!(
                        "Got bytes that are {} bytes long: {:?}",
                        binary.len(),
                        binary
                    );
                } else {
                    let value = U256::from_big_endian(binary);
                    uints.push(value);
                }
            }
        }

        self.sum = uints.into_iter().reduce(|a, b| a + b).unwrap();
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::error::Result<()> {
        self.update_batch(states)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

pub fn uint256_sum() -> AggregateUDF {
    create_udaf(
        "uint256_sum",
        vec![DataType::Binary],
        Arc::new(DataType::Binary),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(UInt256Sum::new()))),
        Arc::new(vec![DataType::Binary]),
    )
}
