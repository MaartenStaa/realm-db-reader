use crate::array::Array;

pub trait Build {
    fn build(node: Array) -> anyhow::Result<Self>
    where
        Self: Sized;
}
