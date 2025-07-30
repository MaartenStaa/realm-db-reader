macro_rules! realm_model_field {
    ($row:ident, $field:ident = $alias:expr) => {
        $row.get($alias)
            .ok_or(::anyhow::anyhow!("Missing field: {:?}", $alias))?
            .clone()
            .try_into()?
    };
    ($row:ident, $field:ident) => {
        realm_model_field!($row, $field = stringify!($field))
    };
}

#[macro_export]
macro_rules! realm_model {
    ($struct:ident => $($field:ident$(= $alias:expr)?),*) => {
        impl<'a> ::core::convert::TryFrom<$crate::table::Row<'a>> for $struct {
            type Error = ::anyhow::Error;

            fn try_from(row: $crate::table::Row<'a>) -> ::anyhow::Result<Self> {
                $(
                let $field = realm_model_field!(row, $field$(= $alias)?);
                )*

                Ok(Self {
                    $(
                        $field,
                    )*
                })
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::table::Row;
    use crate::value::Value;
    use itertools::*;

    #[test]
    fn test_realm_model() {
        struct MyModel {
            id: String,
            foo: Option<String>,
            bar: Option<chrono::DateTime<chrono::Utc>>,
            baz: i64,
            qux: Option<i64>,
            other: bool,
        }

        realm_model!(MyModel => id, foo, bar, baz, qux, other = "!invalid_rust_alias");

        let foo_values = [Some("hello".to_string()), None];
        let bar_values = [Some(chrono::Utc::now()), None];
        let qux_values = [Some(42), None];

        for (foo_value, bar_value, qux_value) in iproduct!(foo_values, bar_values, qux_values) {
            let values: Vec<Value> = vec![
                "id_value".into(),
                foo_value.clone().into(),
                bar_value.into(),
                "extra_field".into(),
                100.into(),
                qux_value.into(),
                true.into(),
                "extra_field".into(),
            ];
            let row = Row::new_with_names(
                &values,
                &[
                    "id",
                    "foo",
                    "bar",
                    "some_other_field",
                    "baz",
                    "qux",
                    "!invalid_rust_alias",
                    "another_field",
                ],
            );

            let my_model: MyModel = row.try_into().unwrap();
            assert_eq!(my_model.id, "id_value");
            assert_eq!(my_model.foo, foo_value);
            assert_eq!(my_model.bar, bar_value);
            assert_eq!(my_model.baz, 100);
            assert_eq!(my_model.qux, qux_value);
            assert!(my_model.other);
        }
    }
}
