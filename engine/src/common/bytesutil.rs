/// Sort a slice of byte slices.
pub fn sort(a: &mut [Vec<u8>]) {
    a.sort();
}

pub fn is_sorted(a: &[Vec<u8>]) -> bool {
    a.is_sorted()
}

/// Performs a binary search for x in the sorted slice a.
pub fn search_bytes(a: &[Vec<u8>], x: &[u8]) -> usize {
    let (mut i, mut j) = (0usize, a.len());
    while i < j {
        let h = i + ((j - i) >> 1);
        if a[h].as_slice() < x {
            i = h + 1; // preserves f(i-1) == false
        } else {
            j = h; // preserves f(j) == true
        }
    }

    i
}

/// Returns true if x is an element of the sorted slice a.
pub fn contains(a: &[Vec<u8>], x: &[u8]) -> bool {
    let n = search_bytes(a, x);
    n < a.len() && (a[n].as_slice() == x)
}
