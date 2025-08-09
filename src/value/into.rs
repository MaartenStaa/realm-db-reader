use std::fmt::Debug;

use anyhow::anyhow;
use chrono::{DateTime, Utc};

use crate::{
    table::Row,
    value::{ARRAY_VALUE_KEY, Backlink, Link, Value},
};

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

        impl<'a> TryFrom<Row<'a>> for $target {
            type Error = anyhow::Error;

            fn try_from(mut value: Row<'a>) -> Result<Self, Self::Error> {
                let Some(value) = value.take(ARRAY_VALUE_KEY) else {
                    return Err(anyhow!(
                        "Expected a row with field `{ARRAY_VALUE_KEY}`, found {value:?}",
                    ));
                };

                value.try_into()
            }
        }
    };
}

value_try_into!(String, String);
value_try_into!(Option<String>, String);
value_try_into!(i64, Int);
value_try_into!(Option<i64>, Int);
value_try_into!(bool, Bool);
value_try_into!(f32, Float);
value_try_into!(f64, Double);
value_try_into!(DateTime<Utc>, Timestamp);
value_try_into!(Option<DateTime<Utc>>, Timestamp);
value_try_into!(Backlink, BackLink);
value_try_into!(Link, Link);
value_try_into!(Option<Link>, Link);

impl<'a, T> TryFrom<Value> for Vec<T>
where
    T: TryFrom<Row<'a>>,
    T::Error: Debug,
{
    type Error = anyhow::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Table(rows) => {
                let mut result = Vec::with_capacity(rows.len());
                for row in rows {
                    result.push(row.try_into().map_err(|e| {
                        anyhow!("Failed to convert value in row to Vec<T>: {e:?}",)
                    })?);
                }

                Ok(result)
            }
            value => Err(anyhow!("Expected a Table value, found {value:?}")),
        }
    }
}
