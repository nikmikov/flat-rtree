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
use std::marker::PhantomData;
use std::ops::Rem;

pub trait Rectangle2D {
    const INVALID: Self;

    // merge with INVALID should return same rectangle
    fn merge(&self, other: &Self) -> Self;

    fn cmp_x(&self, other: &Self) -> Ordering;

    fn cmp_y(&self, other: &Self) -> Ordering;
}

#[allow(clippy::len_without_is_empty)]
pub trait StorageBackend<R, T> {
    fn tree_len(&self) -> usize;

    fn tree_get(&self, index: usize) -> Option<&R>;

    fn data_len(&self) -> usize;

    fn data_get(&self, index: usize) -> Option<&T>;
}

impl<R, T> StorageBackend<R, T> for (Vec<R>, Vec<T>) {
    fn tree_len(&self) -> usize {
        self.0.len()
    }

    fn tree_get(&self, index: usize) -> Option<&R> {
        self.0.get(index)
    }

    fn data_len(&self) -> usize {
        self.1.len()
    }

    fn data_get(&self, index: usize) -> Option<&T> {
        self.1.get(index)
    }
}

pub fn bulk_load<
    R: Rectangle2D + Clone,
    T: Copy + Clone,
    const ELEMENTS_PER_NODE: usize,
>(
    input: Vec<(R, T)>,
) -> FlatRTree<R, T, (Vec<R>, Vec<T>), ELEMENTS_PER_NODE> {
    bulk_load_overlap_minimising_top_down(input)
}

pub struct FlatRTree<
    R: Clone,
    T: Clone,
    S: StorageBackend<R, T>,
    const ELEMENTS_PER_NODE: usize = 4,
> {
    //  data_nodes_offset: usize,
    storage: S,
    data_nodes_offset: usize, // start index in tree where data(leaf) nodes rectangles start
    _marker: (PhantomData<R>, PhantomData<T>),
}

impl<
        R: Clone,
        T: Clone,
        S: StorageBackend<R, T>,
        const ELEMENTS_PER_NODE: usize,
    > FlatRTree<R, T, S, ELEMENTS_PER_NODE>
{
    pub fn new(storage: S) -> Self {
        let tree_height =
            get_tree_height::<ELEMENTS_PER_NODE>(storage.data_len());
        let number_of_tree_nodes =
            get_number_of_tree_nodes::<ELEMENTS_PER_NODE>(tree_height);
        let number_of_data_nodes = ELEMENTS_PER_NODE.pow(tree_height);
        let data_nodes_offset = number_of_tree_nodes - number_of_data_nodes;
        FlatRTree {
            storage,
            data_nodes_offset,
            _marker: Default::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.storage.data_len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_storage(self) -> S {
        self.storage
    }

    // return all elements intersecting bounding rectangle
    pub fn intersects<'a, I: Fn(&R) -> bool + 'a>(
        &'a self,
        intersect: I,
    ) -> impl Iterator<Item = T> + 'a {
        FlatRTreeIntersectIterator::<_, _, _, _, ELEMENTS_PER_NODE>::new(
            &self.storage,
            self.data_nodes_offset,
            intersect,
        )
    }
}

struct FlatRTreeIntersectIterator<
    'a,
    R,
    T: Clone,
    S: StorageBackend<R, T>,
    IntersectFn: Fn(&R) -> bool,
    const ELEMENTS_PER_NODE: usize,
> {
    storage: &'a S,
    index: usize,
    data_nodes_offset: usize,
    intersect: IntersectFn,
    _marker: PhantomData<(R, T)>,
}

impl<
        'a,
        R: Clone,
        T: Clone,
        S: StorageBackend<R, T> + 'a,
        IntersectFn: Fn(&R) -> bool,
        const ELEMENTS_PER_NODE: usize,
    > FlatRTreeIntersectIterator<'a, R, T, S, IntersectFn, ELEMENTS_PER_NODE>
{
    fn new(
        storage: &'a S,
        data_nodes_offset: usize,
        intersect: IntersectFn,
    ) -> Self {
        Self {
            storage,
            index: 0,
            data_nodes_offset,
            intersect,
            _marker: Default::default(),
        }
    }

    fn walk_down(&self, index: &usize) -> Option<usize> {
        if self.is_data_node(index) {
            None
        } else {
            Some(get_first_child_index::<ELEMENTS_PER_NODE>(index))
        }
    }

    fn walk_next(index: &usize) -> Option<usize> {
        if is_last_block_node::<ELEMENTS_PER_NODE>(index) {
            Self::walk_next(&get_parent_index::<ELEMENTS_PER_NODE>(index)?)
        } else {
            Some(index + 1)
        }
    }

    const fn is_data_node(&self, index: &usize) -> bool {
        *index >= self.data_nodes_offset
    }

    fn get_element(&'a self, index: &usize) -> Option<&'a T> {
        if *index < self.data_nodes_offset {
            None
        } else {
            self.storage.data_get(index - self.data_nodes_offset)
        }
    }
}

impl<
        'a,
        R: Clone + 'a,
        T: Clone + 'a,
        S: StorageBackend<R, T>,
        IntersectFn: Fn(&R) -> bool,
        const ELEMENTS_PER_NODE: usize,
    > Iterator
    for FlatRTreeIntersectIterator<'a, R, T, S, IntersectFn, ELEMENTS_PER_NODE>
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let current_index = self.index;
        let rectangle = &self.storage.tree_get(current_index)?;

        if !(self.intersect)(rectangle) {
            // try next element or move up
            self.index = Self::walk_next(&self.index)?;
            self.next()
        } else if self.is_data_node(&current_index) {
            self.index =
                Self::walk_next(&self.index).unwrap_or(self.storage.tree_len());
            self.get_element(&current_index).cloned()
        } else {
            self.index = self.walk_down(&self.index)?;
            self.next()
        }
    }
}

pub fn rectangles_intersect<T: Ord + PartialOrd>(
    a: &([T; 2], [T; 2]),
    b: &([T; 2], [T; 2]),
) -> bool {
    let ([min_x, min_y], [max_x, max_y]) = a;
    let ([other_min_x, other_min_y], [other_max_x, other_max_y]) = b;
    !(other_min_x > max_x
        || other_max_x < min_x
        || other_min_y > max_y
        || other_max_y < min_y)
}

impl Rectangle2D for ([i32; 2], [i32; 2]) {
    const INVALID: Self = ([i32::MAX, i32::MAX], [i32::MIN, i32::MIN]);

    fn merge(&self, other: &Self) -> Self {
        let ([min_x, min_y], [max_x, max_y]) = self;
        let ([other_min_x, other_min_y], [other_max_x, other_max_y]) = other;
        use std::cmp::{max, min};
        (
            [*min(min_x, other_min_x), *min(min_y, other_min_y)],
            [*max(max_x, other_max_x), *max(max_y, other_max_y)],
        )
    }

    fn cmp_x(&self, other: &Self) -> Ordering {
        let ([min_x, _], [max_x, _]) = *self;
        let ([other_min_x, _], [other_max_x, _]) = *other;
        midpoint(min_x, max_x).cmp(&midpoint(other_min_x, other_max_x))
    }

    fn cmp_y(&self, other: &Self) -> Ordering {
        let ([_, min_y], [_, max_y]) = *self;
        let ([_, other_min_y], [_, other_max_y]) = *other;
        midpoint(min_y, max_y).cmp(&midpoint(other_min_y, other_max_y))
    }
}

fn bulk_load_overlap_minimising_top_down<
    R: Rectangle2D + Clone,
    T: Clone + Copy,
    const ELEMENTS_PER_NODE: usize,
>(
    mut input: Vec<(R, T)>,
) -> FlatRTree<R, T, (Vec<R>, Vec<T>), ELEMENTS_PER_NODE> {
    let number_of_data_elements = input.len();
    let tree_height =
        get_tree_height::<ELEMENTS_PER_NODE>(number_of_data_elements);
    let number_of_tree_nodes =
        get_number_of_tree_nodes::<ELEMENTS_PER_NODE>(tree_height);
    let number_of_data_nodes = ELEMENTS_PER_NODE.pow(tree_height);

    // recursively sort by x and y coordinate until we reach last level

    let mut partition_queue: Vec<(u32, &mut [(R, T)])> =
        Vec::with_capacity(100);

    partition_queue.push((tree_height, &mut input[0..number_of_data_elements]));
    while let Some((depth, slice)) = partition_queue.pop() {
        match (tree_height - depth) % 2 {
            0 => slice.sort_unstable_by(|(r1, _), (r2, _)| r1.cmp_x(r2)),
            _ => slice.sort_unstable_by(|(r1, _), (r2, _)| r1.cmp_y(r2)),
        };

        let new_depth = depth - 1;
        if new_depth > 1 {
            let subtree_len = match slice.len() % ELEMENTS_PER_NODE {
                0 => slice.len() / ELEMENTS_PER_NODE,
                _ => 1 + slice.len() / ELEMENTS_PER_NODE,
            };
            for chunk in slice.chunks_mut(subtree_len) {
                partition_queue.push((new_depth, chunk));
            }
        }
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

    FlatRTree::new((tree, data))
}

/// return true if the node is last node in the block
fn is_last_block_node<const ELEMENTS_PER_NODE: usize>(index: &usize) -> bool {
    index.rem(ELEMENTS_PER_NODE) == ELEMENTS_PER_NODE - 1
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
    if number_of_elements < ELEMENTS_PER_NODE {
        1
    } else {
        1 + (number_of_elements - 1).ilog(ELEMENTS_PER_NODE)
    }
}

fn midpoint(p1: i32, p2: i32) -> i32 {
    let left = std::cmp::min(p1, p2);
    let right = std::cmp::max(p1, p2);
    left + (right - left) / 2
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

    type TestRect = ([i32; 2], [i32; 2]);

    fn generate<R: Rng>(
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
                return ([x, y], [x + width, y + height]);
            }
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

    #[test]
    fn zero_height_test() {
        let rect1 = ([0, 0], [1, 1]);
        let rect2 = ([2, 2], [3, 3]);
        let id1 = 1i32;
        let id2 = 2i32;
        let data = vec![(rect1, id1), (rect2, id2)];
        let tree = bulk_load::<_, _, 4>(data);
        assert_eq!(tree.len(), 2);

        let result_1: Vec<i32> = tree
            .intersects(|r| rectangles_intersect(r, &rect1))
            .collect();
        assert_eq!(result_1, vec![id1]);

        let result_2: Vec<i32> = tree
            .intersects(|r| rectangles_intersect(r, &rect2))
            .collect();
        assert_eq!(result_2, vec![id2]);

        let rect3 = rect1.merge(&rect2);
        let result_3: Vec<i32> = tree
            .intersects(|r| rectangles_intersect(r, &rect3))
            .collect();
        assert_eq!(result_3, vec![id1, id2]);
    }

    #[test]
    fn load_and_query_test() {
        const NUM_ELEMENTS: usize = 10_000;

        const MIN_X: i32 = -2000;
        const MIN_Y: i32 = -2000;
        const MAX_X: i32 = 2000;
        const MAX_Y: i32 = 2000;
        const MAX_RECTANGLE_WIDTH: i32 = 100;
        const MAX_RECTANGLE_HEIGHT: i32 = 100;

        let mut rng = rand::thread_rng();

        let data: Vec<(TestRect, usize)> = (0..NUM_ELEMENTS)
            .map(|i| {
                let r = generate(
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

        let tree = bulk_load::<_, _, 4>(data.clone());

        for (rect, index) in &data {
            let mut intersect_query: Vec<usize> =
                tree.intersects(|r| rectangles_intersect(r, rect)).collect();
            intersect_query.sort_unstable();
            assert!(
                intersect_query.contains(index),
                "{index} intersect failed"
            );

            let expected_intersect: Vec<_> = data
                .iter()
                .filter_map(|(r, e)| {
                    if rectangles_intersect(r, rect) {
                        Some(*e)
                    } else {
                        None
                    }
                })
                .collect();

            assert_eq!(intersect_query, expected_intersect);
        }
    }
}
