use chrono::{DateTime, Utc};

use crate::value::Value;

macro_rules! value_try_into {
    (Option<$target:ty>, $source:ident) => {
        impl TryFrom<Value> for Option<$target> {
            type Error = anyhow::Error;

            fn try_from(value: Value) -> Result<Self, Self::Error> {
                match value {
                    Value::$source(val) => Ok(Some(val)),
                    Value::None => Ok(None),
                    _ => Err(anyhow::anyhow!("Expected a {} value", stringify!($source))),
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
                    _ => Err(anyhow::anyhow!("Expected a {} value", stringify!($source))),
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
