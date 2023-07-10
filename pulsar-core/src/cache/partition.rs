use valu3::value::Value;

use super::cache::Cache;

pub type Partition = Cache<Value>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_new() {
        let cache = Partition::new(10);
        assert_eq!(cache.capacity(), 10);
    }

    #[test]
    fn test_table_insert_and_get() {
        let mut cache = Partition::new(10);
        
        cache.insert("key1", Value::from(1));
        cache.insert("key2", Value::from(2));
        assert_eq!(cache.get("key1"), Some(&Value::from(1)));
        assert_eq!(cache.get("key2"), Some(&Value::from(2)));
    }

    #[test]
    fn test_table_remove() {
        let mut cache = Partition::new(10);

        cache.insert("key2", Value::from(2));
        cache.remove("key1");
        assert_eq!(cache.get("key1"), None);
        assert_eq!(cache.get("key2"), Some(&Value::from(2)));
    }

    #[test]
    fn test_table_clear() {
        let mut cache = Partition::new(10);

        cache.insert("key1", Value::from(1));
        cache.insert("key2", Value::from(2));
        cache.clear();
        assert_eq!(cache.get("key1"), None);
        assert_eq!(cache.get("key2"), None);
    }
}
