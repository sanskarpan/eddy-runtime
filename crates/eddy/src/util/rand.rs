//! A tiny per-worker PRNG. Victim selection does not need cryptographic
//! randomness, and keeping this local avoids a runtime dependency on `rand`.

#[derive(Clone, Copy, Debug)]
pub(crate) struct FastRand {
    state: u32,
}

impl FastRand {
    pub(crate) fn new(seed: u32) -> FastRand {
        FastRand {
            state: if seed == 0 { 0x9e37_79b9 } else { seed },
        }
    }

    pub(crate) fn next_u32(&mut self) -> u32 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        self.state = x;
        x
    }

    pub(crate) fn fastrand_n(&mut self, n: u32) -> u32 {
        debug_assert!(n > 0);
        self.next_u32() % n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_seed_is_normalized_and_bounded() {
        let mut rand = FastRand::new(0);
        for _ in 0..100 {
            assert!(rand.fastrand_n(7) < 7);
        }
    }
}
