use flate2::read::GzDecoder;
use rstar::RTreeObject;
use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::ops::ControlFlow;
use std::path::PathBuf;

use criterion::{black_box, criterion_group, criterion_main, Criterion};

pub fn rtree_benchmark(c: &mut Criterion) {
    const NUM_QUERIES: usize = 10_000;

    println!("Loading input");
    let ways = load_ways();
    let step = ways.len() / NUM_QUERIES;
    let queries: Vec<Way> = ways
        .iter()
        .step_by(step)
        .take(NUM_QUERIES)
        .cloned()
        .collect();

    println!("Prepared {} queries", queries.len());

    println!("Loading rstar");
    let rstar = load_rstar(ways.clone());

    println!("Loading sif_rtree");
    let sif_rtree = load_sif_rtree(ways.clone());

    println!("Loading flat rtree");
    let flat_rtree = load_flat_rtree(ways);

    let mut group = c.benchmark_group("Rtree query benchmark");

    group.bench_function("Rstar", |b| {
        b.iter(|| {
            for way in queries.iter() {
                assert!(
                    rstar
                        .locate_in_envelope_intersecting(black_box(
                            &way.envelope(),
                        ))
                        .count()
                        > 0
                );
            }
        })
    });
    group.bench_function("sif::RTree", |b| {
        b.iter(|| {
            for way in queries.iter() {
                use sif_rtree::Object;
                let mut count = 0;
                sif_rtree.look_up_aabb_intersects(&way.aabb(), |_| {
                    count += 1;
                    ControlFlow::Continue(())
                });

                assert!(count > 0);
            }
        })
    });
    group.bench_function("FlatRTree", |b| {
        b.iter(|| {
            for way in queries.iter() {
                assert!(
                    flat_rtree
                        .intersects(black_box(
                            |r: &Rect| r.intersects(&way.bbox)
                        ))
                        .count()
                        > 0
                );
            }
        })
    });
    group.finish();
}

criterion_group!(benches, rtree_benchmark);
criterion_main!(benches);

#[derive(Clone)]
struct Rect([f64; 4]);

#[derive(Clone)]
struct Way {
    id: u32,
    bbox: Rect,
}

impl sif_rtree::Object for Way {
    type Point = [f64; 2];

    fn aabb(&self) -> (Self::Point, Self::Point) {
        let Rect([min_x, min_y, max_x, max_y]) = self.bbox;
        ([min_x, min_y], [max_x, max_y])
    }
}

impl rstar::RTreeObject for Way {
    type Envelope = rstar::AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let Rect([min_x, min_y, max_x, max_y]) = self.bbox;
        rstar::AABB::from_corners([min_x, min_y], [max_x, max_y])
    }
}

fn center(left: f64, right: f64) -> f64 {
    assert!(left <= right);
    left + (right - left) / 2.0
}

impl Rect {
    fn intersects(&self, other: &Self) -> bool {
        let Self([min_x, min_y, max_x, max_y]) = self;
        let Self([other_min_x, other_min_y, other_max_x, other_max_y]) = other;
        !(other_min_x > max_x
            || other_max_x < min_x
            || other_min_y > max_y
            || other_max_y < min_y)
    }
}

impl flat_rtree::Rectangle2D for Rect {
    const INVALID: Self = Self([f64::MAX, f64::MAX, f64::MIN, f64::MIN]);

    fn merge(&self, other: &Self) -> Self {
        let Self([min_x, min_y, max_x, max_y]) = self;
        let Self([other_min_x, other_min_y, other_max_x, other_max_y]) = other;
        Self([
            min_x.min(*other_min_x),
            min_y.min(*other_min_y),
            max_x.max(*other_max_x),
            max_y.max(*other_max_y),
        ])
    }

    fn cmp_x(&self, other: &Self) -> Ordering {
        let Self([min_x, _, max_x, _]) = *self;
        let Self([other_min_x, _, other_max_x, _]) = *other;
        center(min_x, max_x).total_cmp(&center(other_min_x, other_max_x))
    }

    fn cmp_y(&self, other: &Self) -> Ordering {
        let Self([_, min_y, _, max_y]) = *self;
        let Self([_, other_min_y, _, other_max_y]) = *other;
        center(min_y, max_y).total_cmp(&center(other_min_y, other_max_y))
    }
}

fn load_ways() -> Vec<Way> {
    let dataset_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("benches/datasets/switzerland.csv.gz");

    let reader =
        BufReader::new(GzDecoder::new(File::open(&dataset_path).unwrap()));

    let mut result: Vec<Way> = Vec::with_capacity(5_000_000);
    for (index, line) in reader.lines().enumerate() {
        let id: u32 = index as u32;
        let mut bbox = Rect([0.0, 0.0, 0.0, 0.0]);
        line.unwrap()
            .split(',')
            .enumerate()
            .for_each(|(ix, c)| bbox.0[ix] = c.parse().unwrap());

        result.push(Way { id, bbox });
    }
    println!("{} ways loaded", result.len());

    result
}

fn load_rstar(input: Vec<Way>) -> rstar::RTree<Way> {
    rstar::RTree::bulk_load(input)
}

fn load_flat_rtree(
    input: Vec<Way>,
) -> flat_rtree::FlatRTree<Rect, u32, (Vec<Rect>, Vec<u32>), 4> {
    let input: Vec<(Rect, u32)> =
        input.into_iter().map(|x| (x.bbox.clone(), x.id)).collect();
    flat_rtree::bulk_load(input)
}

fn load_sif_rtree(input: Vec<Way>) -> sif_rtree::RTree<Way> {
    sif_rtree::RTree::new(sif_rtree::DEF_NODE_LEN, input)
}
