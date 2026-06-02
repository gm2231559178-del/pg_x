use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);

    pub fn parse(s: &str) -> anyhow::Result<Lsn> {
        let (hi_str, lo_str) = s
            .split_once('/')
            .ok_or_else(|| anyhow::anyhow!("invalid LSN '{}': missing '/'", s))?;
        let hi = u64::from_str_radix(hi_str, 16)
            .map_err(|_| anyhow::anyhow!("invalid LSN high part '{}' in '{}'", hi_str, s))?;
        let lo = u64::from_str_radix(lo_str, 16)
            .map_err(|_| anyhow::anyhow!("invalid LSN low part '{}' in '{}'", lo_str, s))?;
        Ok(Lsn((hi << 32) | lo))
    }

    #[inline]
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }
    #[inline]
    pub fn from_u64(v: u64) -> Self {
        Lsn(v)
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:X}/{:X}",
            (self.0 >> 32) as u32,
            (self.0 & 0xFFFF_FFFF) as u32
        )
    }
}

impl FromStr for Lsn {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Lsn::parse(s)
    }
}

impl From<u64> for Lsn {
    fn from(v: u64) -> Self {
        Lsn(v)
    }
}
impl From<Lsn> for u64 {
    fn from(l: Lsn) -> Self {
        l.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid() {
        let l = Lsn::parse("0/0").unwrap();
        assert!(l.is_zero());

        let l = Lsn::parse("0/AABBCCDD").unwrap();
        assert_eq!(l.as_u64(), 0xAABB_CCDD);

        let l = Lsn::parse("DEAD/BEEF").unwrap();
        assert_eq!(l.as_u64(), (0xDEADu64 << 32) | 0xBEEFu64);
    }

    #[test]
    fn parse_invalid() {
        assert!(Lsn::parse("").is_err());
        assert!(Lsn::parse("no-slash").is_err());
        assert!(Lsn::parse("XX/YY").is_err());
        assert!(Lsn::parse("0/GGGG").is_err());
    }

    #[test]
    fn display_roundtrip() {
        let cases = ["0/0", "DEAD/BEEF", "12345678/9ABCDEF0", "FFFFFFFF/FFFFFFFF"];
        for s in cases {
            let l = Lsn::parse(s).unwrap();
            assert_eq!(l.to_string(), s, "roundtrip failed for '{s}'");
        }
    }

    #[test]
    fn conversions() {
        let l = Lsn::from(42u64);
        assert_eq!(l.as_u64(), 42);
        let back: u64 = l.into();
        assert_eq!(back, 42);

        assert_eq!(Lsn::from_u64(0).is_zero(), true);
        assert_eq!(Lsn::from_u64(1).is_zero(), false);
    }

    #[test]
    fn from_str() {
        let l: Lsn = "0/1".parse().unwrap();
        assert_eq!(l.as_u64(), 1);
    }
}
