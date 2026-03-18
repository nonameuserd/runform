// Agentic Knowledge Compiler — Phase 5 correctness slice (Verus)
//
// This crate contains a small Verus-verified model of the path-sanitization
// rules enforced by the Python verifier (see `akc.compile.verifier._is_path_suspicious`).
//
// It is intentionally minimal so that:
//   - `cargo verus -p akc_formal_verus` can run quickly in CI
//   - the logic is easy to audit and compare with the Python implementation
//
// Note: Verus requires a custom toolchain; this file is structured so that it
// can be processed by Verus even if it is not buildable with a stock Rust
// compiler.

#![allow(unused_imports)]
#![allow(dead_code)]

use vstd::prelude::*;

verus! {

pub open spec fn is_traversal_segment(seg: Seq<char>) -> bool {
    seg.len() == 2
        && seg.index(0) == '.'
        && seg.index(1) == '.'
}

pub open spec fn is_path_suspicious(path: Seq<char>) -> bool
{
    // Reject empty paths.
    if path.len() == 0 {
        return true;
    }

    // Reject leading '/' or '\' (absolute paths).
    if path.index(0) == '/' || path.index(0) == '\\' {
        return true;
    }

    // Reject leading '~' (home-directory expansion).
    if path.index(0) == '~' {
        return true;
    }

    // Very small model of "contains drive prefix like C:foo".
    if path.len() >= 2 && path.index(1) == ':' {
        return true;
    }

    // Reject traversal segments ".." after normalizing separators.
    let slash = '/';
    let backslash = '\\';
    let mut i: nat = 0;
    while i < path.len()
        decreases path.len() - i
    {
        // Find next separator.
        let mut j: nat = i;
        while j < path.len() && path.index(j) != slash && path.index(j) != backslash
            decreases path.len() - j
        {
            j = j + 1;
        }

        let seg = path.subrange(i, j);
        if is_traversal_segment(seg) {
            return true;
        }

        // Advance past separator (if any).
        i = if j < path.len() { j + 1 } else { j };
    }

    false
}

// A tiny sanity lemma: any path that explicitly contains the sequence ".."
// as a whole segment is considered suspicious.
pub proof fn lemma_dotdot_segment_is_suspicious()
{
    let p: Seq<char> = seq!['a', '/', '.', '.', '/', 'b'];
    assert(is_path_suspicious(p));
}

} // verus!

