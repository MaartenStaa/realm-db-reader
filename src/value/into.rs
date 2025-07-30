use std::fmt::Debug;

use anyhow::anyhow;
use chrono::{DateTime, Utc};

use crate::value::{ARRAY_VALUE_KEY, Backlink, Value};

macro_rules! value_try_into {
    (Option<$target:ty>, $source:ident) => {
        impl TryFrom<Value> for Option<$target> {
            type Error = anyhow::Error;

            fn try_from(value: Value) -> Result<Self, Self::Error> {
                match value {
                    Value::$source(val) => Ok(Some(val)),
                    Value::None => Ok(None),
                    value => Err(anyhow::anyhow!(
                        "Expected a {} value, found {value:?}",
                        stringify!($source)
                    )),
                }
            }
        }
    };

    ($target:ty, $source:ident) => {
        impl TryFrom<Value> for $target {
            type Error = anyhow::Error;

            fn try_from(value: Value) -> Result<Self, Self::Error> {
                match value {
                    Value::$source(val) => Ok(val),
                    table => Err(anyhow::anyhow!(
                        "Expected a {} value, found {table:?}",
                        stringify!($source)
                    )),
                }
            }
        }
    };
}

value_try_into!(String, String);
value_try_into!(Option<String>, String);
value_try_into!(i64, Int);
value_try_into!(Option<i64>, Int);
value_try_into!(bool, Bool);
value_try_into!(DateTime<Utc>, Timestamp);
value_try_into!(Option<DateTime<Utc>>, Timestamp);
value_try_into!(Backlink, BackLink);

impl<T> TryFrom<Value> for Vec<T>
where
    T: TryFrom<Value>,
    T::Error: Debug,
{
    type Error = anyhow::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Table(rows) => {
                // Bingo!
                let mut result = Vec::with_capacity(rows.len());
                if !rows
                    .first()
                    .map(|row| row.has_field(ARRAY_VALUE_KEY))
                    .unwrap_or(true)
                {
                    return Err(anyhow!(
                        "Expected a Table with field `{ARRAY_VALUE_KEY}`, found {:?}",
                        rows.first()
                    ));
                }

                for mut row in rows {
                    result.push(
                        row.take(ARRAY_VALUE_KEY)
                            .expect("Expected to find ARRAY_VALUE_KEY in row")
                            .try_into()
                            .map_err(|e| {
                                anyhow!(
                                    "Failed to convert value in row to Vec<T>: {e:?} for row: {row:?}",
                                )
                            })?,
                    );
                }

                Ok(result)
            }
            value => Err(anyhow!("Expected a Table value, found {value:?}")),
        }
    }
}
