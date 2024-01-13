use super::{cache::Cache, partition::Partition};

pub type Table = Cache<Partition>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_new() {
        let cache = Table::new(10);
        assert_eq!(cache.capacity(), 10);
    }
}
