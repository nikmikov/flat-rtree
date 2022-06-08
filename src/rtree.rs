/// Static R-Tree index that doesn't support modifications after creation
///
/// Data structure uses contiguous memory and can be directly memory mapped
/// from serialized binary file
///
/// 1. Can only bulk loaded once, doesn't support insertion
/// 2. Support only trivially copyable data
///
/// Uses top-down loading approach to improve search query performance:
///
/// http://ceur-ws.org/Vol-74/files/FORUM_18.pdf
use std::cmp::Ordering;

pub trait Rectangle2D {
    const INVALID: Self;

    fn intersects(&self, other: &Self) -> bool;

    // merge with INVALID should return same rectangle
    fn merge(&self, other: &Self) -> Self;

    fn cmp_x(&self, other: &Self) -> Ordering;

    fn cmp_y(&self, other: &Self) -> Ordering;
}

pub struct RTree<const ELEMENTS_PER_NODE: usize, R: Rectangle2D, T: Copy> {
    len: usize,
    tree: Vec<R>,
    data: Vec<T>,
}

pub enum BulkLoadStrategy {
    SortTileRecursive,
    OverlapMinimizingTopDown,
}

impl<const ELEMENTS_PER_NODE: usize, R: Rectangle2D + Copy, T: Copy>
    RTree<ELEMENTS_PER_NODE, R, T>
{
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.data.iter()
    }

    /// return all elements intersecting bounding rectangle
    pub fn intersects<'a>(
        &'a self,
        bounding_rectangle: &'a R,
    ) -> impl Iterator<Item = T> + 'a {
        RTreeIterator::new(self, bounding_rectangle)
    }

    pub fn load(input: Vec<(R, T)>, strategy: BulkLoadStrategy) -> Self {
        use BulkLoadStrategy::*;
        match strategy {
            SortTileRecursive => Self::bulk_load_sort_tile_recursive(input),
            OverlapMinimizingTopDown => todo!(),
        }
    }

    fn bulk_load_sort_tile_recursive(mut input: Vec<(R, T)>) -> Self {
        let tree_height = get_tree_height::<ELEMENTS_PER_NODE>(input.len());
        let number_of_tree_nodes =
            get_number_of_tree_nodes::<ELEMENTS_PER_NODE>(tree_height);
        let number_of_data_nodes = ELEMENTS_PER_NODE.pow(tree_height);

        println!("input len: {}, height: {tree_height}, number of tree nodes/ data nodes: {number_of_tree_nodes}/{number_of_data_nodes }", input.len());

        // sort input by X dimension
        input.sort_unstable_by(|(rect1, _), (rect2, _)| rect1.cmp_x(rect2));
        // partition sorted input into `num_slices_x`
        let num_slices_x = (number_of_data_nodes as f64
            / ELEMENTS_PER_NODE as f64)
            .sqrt()
            .ceil() as usize;

        let slice_size =
            (input.len() as f64 / num_slices_x as f64).ceil() as usize;
        println!("Num slices x:{num_slices_x}, slice size: {slice_size}");
        // sort each slice by Y dimension
        for slice_num in 0..num_slices_x {
            let start_ix = slice_num * slice_size;
            let end_ix = std::cmp::min(input.len(), start_ix + slice_size);
            println!("Sorting: {}..{}", start_ix, end_ix);
            input[start_ix..end_ix]
                .sort_unstable_by(|(rect1, _), (rect2, _)| rect1.cmp_y(rect2));
        }

        let mut data = Vec::with_capacity(number_of_data_nodes);
        let mut tree = vec![R::INVALID; number_of_tree_nodes];

        // copy data into leaf nodes
        let data_nodes_offset = number_of_tree_nodes - number_of_data_nodes;
        let tree_data_nodes = &mut tree[data_nodes_offset..];
        for (rectangle, element) in input.into_iter() {
            tree_data_nodes[data.len()] = rectangle;
            data.push(element);
        }

        // go from bottom up and calculate bounding boxes of parent nodes
        let mut level_start_index = data_nodes_offset;
        while level_start_index > 0 {
            let upper_level_start_index =
                get_parent_index::<ELEMENTS_PER_NODE>(&level_start_index)
                    .unwrap_or(0);
            println!("Level index: {level_start_index}, upper: {upper_level_start_index}");
            let mut current_block_start_index = level_start_index;
            for parent_index in upper_level_start_index..level_start_index {
                let current_block_end_index =
                    current_block_start_index + ELEMENTS_PER_NODE;

                tree[parent_index] = tree
                    [current_block_start_index..current_block_end_index]
                    .iter()
                    .fold(R::INVALID, |rect_acc, rect| rect_acc.merge(rect));

                current_block_start_index = current_block_end_index;
            }
            level_start_index = upper_level_start_index
        }

        Self {
            len: data.len(),
            tree,
            data,
        }
    }
}

impl<const ELEMENTS_PER_NODE: usize, R: Rectangle2D + Copy, T: Copy>
    IntoIterator for RTree<ELEMENTS_PER_NODE, R, T>
{
    type Item = T;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

struct RTreeIterator<
    'a,
    const ELEMENTS_PER_NODE: usize,
    R: Rectangle2D,
    T: Copy,
> {
    rtree: &'a RTree<ELEMENTS_PER_NODE, R, T>,
    rectangle: &'a R,
    index: usize,
}

impl<'a, const ELEMENTS_PER_NODE: usize, R: Rectangle2D, T: Copy>
    RTreeIterator<'a, ELEMENTS_PER_NODE, R, T>
{
    fn new(
        rtree: &'a RTree<ELEMENTS_PER_NODE, R, T>,
        rectangle: &'a R,
    ) -> Self {
        Self {
            rtree,
            rectangle,
            index: 0,
        }
    }

    fn walk_down(&self, index: &usize) -> Option<usize> {
        if self.is_data_node(index) {
            None
        } else {
            Some(get_first_child_index::<ELEMENTS_PER_NODE>(index))
        }
    }

    fn walk_next(&self, index: &usize) -> Option<usize> {
        if is_last_block_node::<ELEMENTS_PER_NODE>(index) {
            self.walk_next(&get_parent_index::<ELEMENTS_PER_NODE>(index)?)
        } else {
            Some(index + 1)
        }
    }

    fn is_data_node(&self, index: &usize) -> bool {
        *index >= self.data_nodes_start_index()
    }

    fn get_element(&'a self, index: &usize) -> Option<T> {
        let data_nodes_start_index = self.data_nodes_start_index();
        if *index < data_nodes_start_index {
            None
        } else {
            self.rtree.data.get(index - data_nodes_start_index).copied()
        }
    }

    fn data_nodes_start_index(&self) -> usize {
        debug_assert!(self.rtree.tree.len() > self.rtree.data.len());
        self.rtree.tree.len() - self.rtree.data.len()
    }
}

impl<'a, const ELEMENTS_PER_NODE: usize, R: Rectangle2D, T: Copy> Iterator
    for RTreeIterator<'a, ELEMENTS_PER_NODE, R, T>
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.index;
        let intersects = self.rtree.tree.get(index)?.intersects(self.rectangle);

        if !intersects {
            // try next element or move up
            self.index = self.walk_next(&self.index)?;
            self.next()
        } else if self.is_data_node(&self.index) {
            self.index = self.walk_next(&self.index)?;
            self.get_element(&index)
        } else {
            self.index = self.walk_down(&self.index)?;
            self.next()
        }
    }
}

/// return true if the node is last node in the block
fn is_last_block_node<const ELEMENTS_PER_NODE: usize>(index: &usize) -> bool {
    index % ELEMENTS_PER_NODE == ELEMENTS_PER_NODE - 1
}

/// return index of first child of the node located at given index
fn get_first_child_index<const ELEMENTS_PER_NODE: usize>(
    parent_index: &usize,
) -> usize {
    ELEMENTS_PER_NODE * (parent_index + 1)
}

/// return index of parent of the child node located at given index
fn get_parent_index<const ELEMENTS_PER_NODE: usize>(
    child_index: &usize,
) -> Option<usize> {
    match child_index / ELEMENTS_PER_NODE {
        0 => None,
        n => Some(n - 1),
    }
}

fn get_tree_height<const ELEMENTS_PER_NODE: usize>(
    number_of_elements: usize,
) -> u32 {
    // TODO: this should use int_log when it's stabilised
    // https://github.com/rust-lang/rust/issues/70887
    if number_of_elements < ELEMENTS_PER_NODE {
        1
    } else {
        let value_f64 =
            (number_of_elements as f64).log(ELEMENTS_PER_NODE as f64);
        value_f64.ceil() as u32
    }
}

/// return total number of nodes required for a tree of given height and arity
///
/// formula: (arity^1 + arity^2 + arity^2 + .. + arity ^ n)
///           or
///           arity * (arity^n - 1) / (arity - 1)
const fn get_number_of_tree_nodes<const ELEMENTS_PER_NODE: usize>(
    height: u32,
) -> usize {
    debug_assert!(ELEMENTS_PER_NODE > 1);
    ELEMENTS_PER_NODE * (ELEMENTS_PER_NODE.pow(height) - 1)
        / (ELEMENTS_PER_NODE - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::prelude::*;

    #[derive(Debug, Clone, Copy)]
    struct TestRect(i32, i32, i32, i32);

    fn center(left: i32, right: i32) -> i32 {
        assert!(left <= right);
        left + (right - left) / 2
    }

    impl TestRect {}

    impl Rectangle2D for TestRect {
        const INVALID: Self = TestRect(i32::MAX, i32::MAX, i32::MIN, i32::MIN);

        fn intersects(&self, other: &Self) -> bool {
            let Self(min_x, min_y, max_x, max_y) = self;
            let Self(other_min_x, other_min_y, other_max_x, other_max_y) =
                other;
            !(other_min_x > max_x
                && other_max_x < min_x
                && other_min_y > max_y
                && other_max_y < min_y)
        }

        fn merge(&self, other: &Self) -> Self {
            let Self(min_x, min_y, max_x, max_y) = self;
            let Self(other_min_x, other_min_y, other_max_x, other_max_y) =
                other;
            use std::cmp::{max, min};
            Self(
                *min(min_x, other_min_x),
                *min(min_y, other_min_y),
                *max(max_x, other_max_x),
                *max(max_y, other_max_y),
            )
        }

        fn cmp_x(&self, other: &Self) -> Ordering {
            let Self(min_x, _, max_x, _) = *self;
            let Self(other_min_x, _, other_max_x, _) = *other;
            center(min_x, max_x).cmp(&center(other_min_x, other_max_x))
        }

        fn cmp_y(&self, other: &Self) -> Ordering {
            let Self(_, min_y, _, max_y) = *self;
            let Self(_, other_min_y, _, other_max_y) = *other;
            center(min_y, max_y).cmp(&center(other_min_y, other_max_y))
        }
    }

    #[test]
    fn get_tree_height_test() {
        assert_eq!(get_tree_height::<4>(1), 1);
        assert_eq!(get_tree_height::<4>(2), 1);
        assert_eq!(get_tree_height::<4>(3), 1);
        assert_eq!(get_tree_height::<4>(4), 1);
        assert_eq!(get_tree_height::<4>(5), 2);
        assert_eq!(get_tree_height::<4>(16), 2);
        assert_eq!(get_tree_height::<4>(17), 3);
        assert_eq!(get_tree_height::<4>(64), 3);
        assert_eq!(get_tree_height::<4>(65), 4);
        assert_eq!(get_tree_height::<4>(1_000_000), 10);
    }

    #[test]
    fn get_number_of_tree_nodes_test() {
        assert_eq!(get_number_of_tree_nodes::<4>(1), 4);
        assert_eq!(get_number_of_tree_nodes::<4>(2), 20);
        assert_eq!(get_number_of_tree_nodes::<4>(3), 84);
        assert_eq!(get_number_of_tree_nodes::<4>(4), 340);
        assert_eq!(get_number_of_tree_nodes::<4>(5), 1364);
        assert_eq!(get_number_of_tree_nodes::<4>(6), 5460);
        assert_eq!(get_number_of_tree_nodes::<4>(7), 21844);
        assert_eq!(get_number_of_tree_nodes::<4>(8), 87380);
        assert_eq!(get_number_of_tree_nodes::<4>(9), 349524);
        assert_eq!(get_number_of_tree_nodes::<4>(10), 1398100);
    }

    #[test]
    fn get_first_child_index_test() {
        assert_eq!(get_first_child_index::<4>(&0), 4);
        assert_eq!(get_first_child_index::<4>(&1), 8);
        assert_eq!(get_first_child_index::<4>(&2), 12);
        assert_eq!(get_first_child_index::<4>(&3), 16);
        assert_eq!(get_first_child_index::<4>(&4), 20);
        assert_eq!(get_first_child_index::<4>(&5), 24);
    }

    #[test]
    fn get_parent_index_test() {
        assert_eq!(get_parent_index::<4>(&0), None);
        assert_eq!(get_parent_index::<4>(&1), None);
        assert_eq!(get_parent_index::<4>(&2), None);
        assert_eq!(get_parent_index::<4>(&3), None);
        assert_eq!(get_parent_index::<4>(&4), Some(0));
        assert_eq!(get_parent_index::<4>(&5), Some(0));
        assert_eq!(get_parent_index::<4>(&6), Some(0));
        assert_eq!(get_parent_index::<4>(&7), Some(0));
        assert_eq!(get_parent_index::<4>(&8), Some(1));
        assert_eq!(get_parent_index::<4>(&9), Some(1));
        assert_eq!(get_parent_index::<4>(&10), Some(1));
        assert_eq!(get_parent_index::<4>(&11), Some(1));
        assert_eq!(get_parent_index::<4>(&12), Some(2));
        assert_eq!(get_parent_index::<4>(&13), Some(2));
        assert_eq!(get_parent_index::<4>(&14), Some(2));
        assert_eq!(get_parent_index::<4>(&15), Some(2));
        assert_eq!(get_parent_index::<4>(&16), Some(3));
        assert_eq!(get_parent_index::<4>(&17), Some(3));
        assert_eq!(get_parent_index::<4>(&18), Some(3));
        assert_eq!(get_parent_index::<4>(&19), Some(3));
        assert_eq!(get_parent_index::<4>(&20), Some(4));
    }

    fn generate_rectangle<R: Rng>(
        rng: &mut R,
        min_x: i32,
        min_y: i32,
        max_x: i32,
        max_y: i32,
        max_width: i32,
        max_height: i32,
    ) -> TestRect {
        loop {
            let x = rng.gen_range(min_x..=max_x);
            let y = rng.gen_range(min_y..=max_y);
            // center coordinates
            let width = rng.gen_range(1..=max_width);
            let height = rng.gen_range(1..=max_height);

            if x + width <= max_x && y + height <= max_height {
                return TestRect(x, y, x + width, y + height);
            }
        }
    }

    #[test]
    fn load_test() {
        const NUM_ELEMENTS: usize = 16;

        const MIN_X: i32 = 0;
        const MIN_Y: i32 = 0;
        const MAX_X: i32 = 1000;
        const MAX_Y: i32 = 1000;
        const MAX_RECTANGLE_WIDTH: i32 = 100;
        const MAX_RECTANGLE_HEIGHT: i32 = 100;

        let mut rng = rand::thread_rng();

        let data: Vec<(TestRect, usize)> = (0..NUM_ELEMENTS)
            .map(|i| {
                let r = generate_rectangle(
                    &mut rng,
                    MIN_X,
                    MIN_Y,
                    MAX_X,
                    MAX_Y,
                    MAX_RECTANGLE_WIDTH,
                    MAX_RECTANGLE_HEIGHT,
                );
                (r, i)
            })
            .collect();

        let tree =
            RTree::<4, _, _>::load(data, BulkLoadStrategy::SortTileRecursive);

        println!("{:?}", tree.tree[0..].iter().collect::<Vec<&TestRect>>());
    }
}
