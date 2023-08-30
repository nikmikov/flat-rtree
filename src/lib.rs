mod flat_rtree;

pub use crate::flat_rtree::{
    bulk_load, rectangles_intersect, FlatRTree, Rectangle2D, StorageBackend,
};
