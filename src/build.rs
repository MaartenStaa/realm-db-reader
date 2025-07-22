use crate::array::ArrayBasic;

pub trait Build {
    fn build(node: ArrayBasic) -> anyhow::Result<Self>
    where
        Self: Sized;
}
