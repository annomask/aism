//! Ordered tree construction and boundary joins, independent of file IO.

use super::page_group::validate_record;
use super::{FAN_OUT, LEVEL_BLOCK, MAX_LEVELS, MIN_CHILDREN, Record, Result, invalid};

/// An implicit root with at most 64 same-height subtrees. Only this list may
/// contain fewer than 32 children; persisted internal nodes may not.
#[derive(Clone, Debug)]
pub(crate) struct Forest {
    pub(crate) roots: Vec<Record>,
}
impl Forest {
    pub(crate) fn new(roots: Vec<Record>) -> Result<Self> {
        if roots.is_empty() || roots.len() > FAN_OUT {
            return Err(invalid("invalid root count"));
        }
        let first = roots[0];
        if !(LEVEL_BLOCK..=MAX_LEVELS).contains(&first.level) {
            return Err(invalid("invalid root level"));
        }
        for (i, record) in roots.iter().enumerate() {
            validate_record(record, first.values.field_type())?;
            if record.seq_no != first.seq_no
                || record.level != first.level
                || (i > 0 && roots[i - 1].pos.end >= record.pos.start)
            {
                return Err(invalid("invalid root identity or order"));
            }
        }
        Ok(Self { roots })
    }
    pub(crate) fn level(&self) -> u8 {
        self.roots[0].level
    }
}

#[derive(Debug)]
pub(crate) struct NewNode {
    pub(crate) record: Record,
    pub(crate) children: Vec<Record>,
}

/// Only replaced ancestors are removed. Descendants of reused subtrees remain.
pub(crate) struct JoinPlan {
    pub(crate) forest: Forest,
    pub(crate) removed: Vec<Record>,
    pub(crate) added: Vec<NewNode>,
}

/// Joins strictly ordered, nonoverlapping inputs. Loading children is limited
/// to the facing boundary of the taller tree. Overlap requires raw-data repair.
pub(crate) fn join_ordered(
    left: Forest,
    right: Forest,
    mut load_children: impl FnMut(&Record) -> Result<Vec<Record>>,
) -> Result<JoinPlan> {
    let left = Forest::new(left.roots)?;
    let right = Forest::new(right.roots)?;
    if left.roots[0].seq_no != right.roots[0].seq_no
        || left.roots[0].values.field_type() != right.roots[0].values.field_type()
        || left.roots.last().unwrap().pos.end >= right.roots[0].pos.start
    {
        return Err(invalid(
            "tree join requires matching, disjoint ordered inputs",
        ));
    }
    let mut removed = Vec::new();
    let mut added = Vec::new();
    let forest = join(left, right, &mut load_children, &mut removed, &mut added)?;
    Ok(JoinPlan {
        forest,
        removed,
        added,
    })
}

fn children(
    parent: &Record,
    load: &mut impl FnMut(&Record) -> Result<Vec<Record>>,
) -> Result<Forest> {
    let forest = Forest::new(load(parent)?)?;
    if forest.roots.len() != parent.pos.child_count as usize
        || forest.level() + 1 != parent.level
        || forest.roots[0].seq_no != parent.seq_no
        || forest.roots[0].values.field_type() != parent.values.field_type()
        || forest.roots[0].pos.start != parent.pos.start
        || forest.roots.last().unwrap().pos.end != parent.pos.end
    {
        return Err(invalid("incomplete parent coverage during tree join"));
    }
    Ok(forest)
}

fn summarize(children: Vec<Record>, added: &mut Vec<NewNode>) -> Result<Record> {
    let record = Record::summarize(&children)?;
    added.push(NewNode { record, children });
    Ok(record)
}

fn cap_roots(roots: Vec<Record>, added: &mut Vec<NewNode>) -> Result<Forest> {
    if roots.len() <= FAN_OUT {
        return Forest::new(roots);
    }
    if roots.len() > 2 * FAN_OUT || roots[0].level == MAX_LEVELS {
        return Err(super::Error::TreeCapacityExceeded);
    }
    let split = roots.len() / 2;
    Forest::new(vec![
        summarize(roots[..split].to_vec(), added)?,
        summarize(roots[split..].to_vec(), added)?,
    ])
}

fn replace_boundary(
    joined: Forest,
    parent_level: u8,
    added: &mut Vec<NewNode>,
) -> Result<Vec<Record>> {
    if joined.level() == parent_level {
        return Ok(joined.roots);
    }
    if joined.level() + 1 != parent_level || joined.roots.len() < MIN_CHILDREN {
        return Err(invalid("underfull internal boundary after join"));
    }
    Ok(vec![summarize(joined.roots, added)?])
}

fn join(
    mut left: Forest,
    mut right: Forest,
    load: &mut impl FnMut(&Record) -> Result<Vec<Record>>,
    removed: &mut Vec<Record>,
    added: &mut Vec<NewNode>,
) -> Result<Forest> {
    match left.level().cmp(&right.level()) {
        std::cmp::Ordering::Equal => {
            left.roots.extend(right.roots);
            cap_roots(left.roots, added)
        }
        std::cmp::Ordering::Greater => {
            let parent = left.roots.pop().unwrap();
            let boundary = children(&parent, load)?;
            let joined = join(boundary, right, load, removed, added)?;
            removed.push(parent);
            left.roots
                .extend(replace_boundary(joined, parent.level, added)?);
            cap_roots(left.roots, added)
        }
        std::cmp::Ordering::Less => {
            let parent = right.roots.remove(0);
            let boundary = children(&parent, load)?;
            let joined = join(left, boundary, load, removed, added)?;
            removed.push(parent);
            let mut roots = replace_boundary(joined, parent.level, added)?;
            roots.extend(right.roots);
            cap_roots(roots, added)
        }
    }
}
