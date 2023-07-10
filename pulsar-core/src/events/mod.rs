use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use valu3::value::Value;

pub struct Events {
    events: HashMap<String, Vec<Box<dyn Fn(Option<&'static str>, Option<Value>) + Send + Sync>>>,
}

impl Events {
    pub fn build() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self::new()))
    }

    pub fn new() -> Self {
        Self {
            events: HashMap::new(),
        }
    }

    pub fn on(
        &mut self,
        event_name: String,
        callback: Box<dyn Fn(Option<&'static str>, Option<Value>) + Send + Sync>,
    ) {
        let callbacks = self.events.entry(event_name).or_insert(Vec::new());
        callbacks.push(callback);
    }

    pub fn emit(&mut self, event_name: String, err: Option<&'static str>, data: Option<Value>)
    {
        if let Some(callbacks) = self.events.get_mut(&event_name) {
            for callback in callbacks {
                callback(err.clone(), data.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use valu3::traits::ToValueBehavior;

    use super::*;

    // create test with assert
    #[test]
    fn test_events() {
        let mut events = Events::new();
        let event_name = "test".to_string();
        events.on(event_name.clone(), Box::new(move |err, data| {
            assert_eq!(err, None);
            assert_eq!(data, Some("ok".to_value()));
        }));
        events.emit(event_name.clone(), None, Some("ok".to_value()));
    }

    fn test_events_error() {
        let mut events = Events::new();
        let event_name = "test".to_string();
        events.on(event_name.clone(), Box::new(move |err, data| {
            assert_eq!(err, Some("error"));
            assert_eq!(data, None);
        }));
        events.emit(event_name.clone(), Some("error"), None);
    }
}
