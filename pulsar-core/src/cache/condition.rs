use regex::Regex;
use std::fmt::{self, Display, Formatter};
use valu3::prelude::*;

#[derive(ToValue, FromValue, Clone)]
pub enum Operator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Like,
    NotLike,
    In,
    NotIn,
    Between,
    NotBetween,
    IsNull,
    IsNotNull,
    Regex,
    NotRegex,
}

#[derive(ToValue, FromValue, Clone, PartialEq)]
pub enum LogicalOperator {
    And,
    Or,
}

#[derive(Clone, FromValue, ToValue)]
pub struct Condition {
    pub operator: Operator,
    pub left: Value,
    pub right: Value,
}

impl Condition {
    pub fn new<L, R>(operator: Operator, left: L, right: R) -> Self
    where
        L: Into<Value>,
        R: Into<Value>,
    {
        Self {
            operator,
            left: left.into(),
            right: right.into(),
        }
    }
}

#[derive(Clone)]
pub enum ConditionToken {
    Condition(Condition),
    LogicalOperator(LogicalOperator),
}

impl PrimitiveType for ConditionToken {}

impl FromValueBehavior for ConditionToken {
    type Item = Self;

    fn from_value(value: Value) -> Option<Self::Item> {
        match value.as_str() {
            "And" => Some(ConditionToken::LogicalOperator(LogicalOperator::And)),
            "Or" => Some(ConditionToken::LogicalOperator(LogicalOperator::Or)),
            _ => {
                let condition = Condition::from_value(value)?;
                Some(ConditionToken::Condition(condition))
            }
        }
    }
}

impl ToValueBehavior for ConditionToken {
    fn to_value(&self) -> Value {
        match self {
            ConditionToken::Condition(condition) => condition.to_value(),
            ConditionToken::LogicalOperator(operator) => operator.to_value(),
        }
    }
}

#[derive(Clone, FromValue, ToValue)]
pub struct ConditionGroup {
    pub conditions: Vec<ConditionToken>,
}

#[derive(Clone)]
pub enum ConditionTreeToken {
    ConditionGroup(ConditionGroup),
    LogicalOperator(LogicalOperator),
}

impl PrimitiveType for ConditionTreeToken {}

impl FromValueBehavior for ConditionTreeToken {
    type Item = Self;

    fn from_value(value: Value) -> Option<Self::Item> {
        match value.as_str() {
            "And" => Some(ConditionTreeToken::LogicalOperator(LogicalOperator::And)),
            "Or" => Some(ConditionTreeToken::LogicalOperator(LogicalOperator::Or)),
            _ => {
                let condition = ConditionGroup::from_value(value)?;
                Some(ConditionTreeToken::ConditionGroup(condition))
            }
        }
    }
}

impl ToValueBehavior for ConditionTreeToken {
    fn to_value(&self) -> Value {
        match self {
            ConditionTreeToken::ConditionGroup(condition) => condition.to_value(),
            ConditionTreeToken::LogicalOperator(operator) => operator.to_value(),
        }
    }
}

#[derive(Default)]
pub struct ConditionTree {
    pub conditions: Vec<ConditionTreeToken>,
}

pub enum Where {
    ConditionTree(ConditionTree),
    ConditionGroup(ConditionGroup),
    Condition(Condition),
}

#[derive(Debug)]
pub enum Error {
    LeftConditionNotFound,
    RightConditionNotFound,
    LeftConditionNotString,
    RightConditionNotString,
    ConditionVariableNotFound,
    BetweenConditionInvalid,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Error::LeftConditionNotFound => write!(f, "Left condition not found"),
            Error::RightConditionNotFound => write!(f, "Right condition not found"),
            Error::LeftConditionNotString => write!(f, "Left condition not string"),
            Error::RightConditionNotString => write!(f, "Right condition not string"),
            Error::ConditionVariableNotFound => write!(f, "Condition variable not found"),
            Error::BetweenConditionInvalid => write!(f, "Between condition invalid"),
        }
    }
}

impl Where {
    pub fn condition<L, R>(operator: Operator, left: L, right: R) -> Self
    where
        L: Into<Value>,
        R: Into<Value>,
    {
        Self::Condition(Condition::new(operator, left, right))
    }

    pub fn condition_group(conditions: Vec<ConditionToken>) -> Self {
        Self::ConditionGroup(ConditionGroup { conditions })
    }

    pub fn condition_tree(conditions: Vec<ConditionTreeToken>) -> Self {
        Self::ConditionTree(ConditionTree { conditions })
    }

    pub fn execute(&self, value: &Value) -> Result<Option<Value>, Error> {
        match self {
            Where::ConditionTree(condition_tree) => {
                Self::execute_condition_tree(condition_tree, value)
            }
            Where::ConditionGroup(condition_group) => {
                Self::execute_condition_group(condition_group, value)
            }
            Where::Condition(condition) => Self::execute_condition(condition.clone(), value),
        }
    }

    pub fn execute_condition_tree(
        condition_tree: &ConditionTree,
        value: &Value,
    ) -> Result<Option<Value>, Error> {
        let mut result = None;

        for condition in &condition_tree.conditions {
            match condition {
                ConditionTreeToken::ConditionGroup(condition_group) => {
                    let condition_result = Self::execute_condition_group(condition_group, value)?;

                    if let Some(condition_result) = condition_result {
                        result = Some(condition_result);
                    }
                }
                ConditionTreeToken::LogicalOperator(operator) => match operator {
                    LogicalOperator::And => {
                        if result.is_none() {
                            return Ok(None);
                        }
                    }
                    LogicalOperator::Or => {}
                },
            }
        }

        Ok(result)
    }

    fn execute_condition_group(
        condition_group: &ConditionGroup,
        value: &Value,
    ) -> Result<Option<Value>, Error> {
        let mut result = None;

        for condition in &condition_group.conditions {
            match condition {
                ConditionToken::Condition(condition) => {
                    let condition_result = Self::execute_condition(condition.clone(), value)?;

                    if let Some(condition_result) = condition_result {
                        result = Some(condition_result);
                    }
                }
                ConditionToken::LogicalOperator(operator) => match operator {
                    LogicalOperator::And => {
                        if result.is_none() {
                            return Ok(None);
                        }
                    }
                    LogicalOperator::Or => {}
                },
            }
        }

        Ok(result)
    }

    pub fn execute_condition(condition: Condition, value: &Value) -> Result<Option<Value>, Error> {
        let value_left = match Self::resolve_condition_variable(&condition.left.to_value(), &value)
        {
            Ok(val) => val,
            Err(_) => return Err(Error::LeftConditionNotFound),
        };

        let value_right =
            match Self::resolve_condition_variable(&condition.right.to_value(), &value) {
                Ok(val) => val,
                Err(_) => return Err(Error::RightConditionNotFound),
            };

        match condition.operator {
            Operator::Equal => {
                if value_left.eq(&value_right) {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            Operator::NotEqual => {
                if value_left.ne(&value_right) {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            Operator::GreaterThan => {
                if value_left.gt(&value_right) {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            Operator::GreaterThanOrEqual => {
                if value_left.ge(&value_right) {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            Operator::LessThan => {
                if value_left.lt(&value_right) {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            Operator::LessThanOrEqual => {
                if value_left.le(&value_right) {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            Operator::Like => {
                if Self::operator_like(&value_left, &value_right)? {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            Operator::NotLike => {
                if !Self::operator_like(&value_left, &value_right)? {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            Operator::In => {
                if Self::operator_in(&value_left, &value_right)? {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            Operator::NotIn => {
                if !Self::operator_in(&value_left, &value_right)? {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            Operator::Between => {
                if Self::operator_between(&value_left, &value_right)? {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            Operator::NotBetween => {
                if !Self::operator_between(&value_left, &value_right)? {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            Operator::IsNull => {
                if value_left.is_null() {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            Operator::IsNotNull => {
                if !value_left.is_null() {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            Operator::Regex => {
                if Self::operator_regex(&value_left, &value_right)? {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            Operator::NotRegex => {
                if !Self::operator_regex(&value_left, &value_right)? {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    pub fn resolve_condition_variable(variable: &Value, value: &Value) -> Result<Value, Error> {
        if variable.is_string() {
            match Self::extract_sql_string(&variable.as_string()) {
                Some(val) => Ok(Value::from(val)),
                None => {
                    let variable_str = variable.as_str();

                    match value.get(variable_str) {
                        Some(val) => Ok(val.clone()),
                        None => match Value::try_from(variable_str) {
                            Ok(val) => Ok(val),
                            Err(_) => Err(Error::ConditionVariableNotFound),
                        },
                    }
                }
            }
        } else {
            Ok(variable.clone())
        }
    }

    // if value is beteween ' ' ou " " then return content else return None
    pub fn extract_sql_string(value: &String) -> Option<String> {
        let mut chars = value.chars();
        let first_char = chars.next();
        let last_char = chars.next_back();

        if first_char == last_char && (first_char == Some('\'') || first_char == Some('"')) {
            let mut result = String::new();
            for c in value.chars().skip(1).take(value.len() - 2) {
                result.push(c);
            }
            Some(result)
        } else {
            None
        }
    }

    pub fn operator_like(value_left: &Value, value_right: &Value) -> Result<bool, Error> {
        let left = match value_left.as_string_b() {
            Some(val) => val.as_string(),
            None => return Err(Error::LeftConditionNotString),
        };
        let mut right = match value_right.as_string_b() {
            Some(val) => val.as_string(),
            None => return Err(Error::RightConditionNotString),
        };

        // no use regex
        if right.starts_with('%') && right.ends_with('%') {
            right = right.trim_matches('%').to_string();
            if left.contains(&right) {
                return Ok(true);
            }
        } else if right.starts_with('%') {
            right = right.trim_start_matches('%').to_string();
            if left.ends_with(&right) {
                return Ok(true);
            }
        } else if right.ends_with('%') {
            right = right.trim_end_matches('%').to_string();
            if left.starts_with(&right) {
                return Ok(true);
            }
        } else {
            if left.eq(&right) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn operator_regex(value_left: &Value, value_right: &Value) -> Result<bool, Error> {
        let left = match value_left.as_string_b() {
            Some(val) => val.as_string(),
            None => return Err(Error::LeftConditionNotString),
        };
        let right = match value_right.as_string_b() {
            Some(val) => val.as_string(),
            None => return Err(Error::RightConditionNotString),
        };

        let re = Regex::new(&right).unwrap();
        if re.is_match(&left) {
            return Ok(true);
        }

        Ok(false)
    }

    pub fn operator_in(value_left: &Value, value_right: &Value) -> Result<bool, Error> {
        let left = match value_left.as_string_b() {
            Some(val) => val.as_string(),
            None => return Err(Error::LeftConditionNotString),
        };
        let right = match value_right.as_array() {
            Some(val) => val,
            None => return Err(Error::RightConditionNotString),
        };

        for value in right {
            let value = match value.as_string_b() {
                Some(val) => val.as_string(),
                None => return Err(Error::RightConditionNotString),
            };
            if left.eq(&value) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub fn operator_between(value_left: &Value, value_right: &Value) -> Result<bool, Error> {
        if !value_right.is_array() && value_right.len() != 2 {
            return Err(Error::BetweenConditionInvalid);
        }

        let value1 = match value_right.get(0) {
            Some(val) => val,
            None => return Err(Error::BetweenConditionInvalid),
        };

        let value2 = match value_right.get(1) {
            Some(val) => val,
            None => return Err(Error::BetweenConditionInvalid),
        };

        if value_left.ge(&value1) && value_left.le(&value2) {
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[macro_export]
macro_rules! sql_string {
    ($string:expr) => {
        format!("'{}'", $string)
    };
}

#[cfg(test)]
mod tests {
    use super::{
        Condition, ConditionGroup, ConditionTree, ConditionTreeToken, LogicalOperator, Operator,
        Where,
    };
    use valu3::prelude::*;

    #[test]
    fn test_condition_equal() {
        let where_token = Where::condition(Operator::Equal, "name", sql_string!("John"));

        let value = Value::from(vec![("name", "John")]);
        let result = match where_token.execute(&value) {
            Ok(result) => result,
            Err(err) => panic!("{}", err),
        };

        assert_eq!(result, Some(Value::from(vec![("name", "John")])));
    }

    #[test]
    fn test_condition_not_equal() {
        let where_token = Where::condition(Operator::NotEqual, "name", sql_string!("John"));

        let value = Value::from(vec![("name", "John")]);
        let result = match where_token.execute(&value) {
            Ok(result) => result,
            Err(err) => panic!("{}", err),
        };

        assert_eq!(result, None);
    }

    #[test]
    fn test_condition_greater_than() {
        let where_token = Where::condition(Operator::GreaterThan, "age", 18);

        let value = Value::from(vec![("age", 19)]);
        let result = match where_token.execute(&value) {
            Ok(result) => result,
            Err(err) => panic!("{}", err),
        };

        assert_eq!(result, Some(Value::from(vec![("age", 19)])));
    }

    #[test]
    fn test_condition_greater_than_or_equal() {
        let where_token = Where::condition(Operator::GreaterThanOrEqual, "age", 18);

        let value = Value::from(vec![("age", 18)]);
        let result = match where_token.execute(&value) {
            Ok(result) => result,
            Err(err) => panic!("{}", err),
        };

        assert_eq!(result, Some(Value::from(vec![("age", 18)])));
    }

    #[test]
    fn test_condition_less_than() {
        let where_token = Where::condition(Operator::LessThan, "age", 18);

        let value = Value::from(vec![("age", 17)]);
        let result = match where_token.execute(&value) {
            Ok(result) => result,
            Err(err) => panic!("{}", err),
        };

        assert_eq!(result, Some(Value::from(vec![("age", 17)])));
    }

    #[test]
    fn test_condition_less_than_or_equal() {
        let where_token = Where::condition(Operator::LessThanOrEqual, "age", 18);

        let value = Value::from(vec![("age", 18)]);
        let result = match where_token.execute(&value) {
            Ok(result) => result,
            Err(err) => panic!("{}", err),
        };

        assert_eq!(result, Some(Value::from(vec![("age", 18)])));
    }

    #[test]
    fn test_condition_like() {
        let where_token = Where::condition(Operator::Like, "name", sql_string!("J%"));

        let value = Value::from(vec![("name", "John")]);
        let result = match where_token.execute(&value) {
            Ok(result) => result,
            Err(err) => panic!("{}", err),
        };

        assert_eq!(result, Some(Value::from(vec![("name", "John")])));
    }

    #[test]
    fn test_condition_not_like() {
        let where_token = Where::condition(Operator::NotLike, "name", sql_string!("J%"));

        let value = Value::from(vec![("name", "John")]);
        let result = match where_token.execute(&value) {
            Ok(result) => result,
            Err(err) => panic!("{}", err),
        };

        assert_eq!(result, None);
    }

    #[test]
    fn test_condition_in() {
        let where_token = Where::condition(Operator::In, "name", Value::from(vec!["John", "Jane"]));

        let value = Value::from(vec![("name", "John")]);
        let result = match where_token.execute(&value) {
            Ok(result) => result,
            Err(err) => panic!("{}", err),
        };

        assert_eq!(result, Some(Value::from(vec![("name", "John")])));
    }

    #[test]
    fn test_condition_not_in() {
        let where_token =
            Where::condition(Operator::NotIn, "name", Value::from(vec!["John", "Jane"]));

        let value = Value::from(vec![("name", "John")]);
        let result = match where_token.execute(&value) {
            Ok(result) => result,
            Err(err) => panic!("{}", err),
        };

        assert_eq!(result, None);
    }

    #[test]
    fn test_condition_between() {
        let where_token = Where::condition(Operator::Between, "age", Value::from(vec![18, 20]));

        let value = Value::from(vec![("age", 19)]);
        let result = match where_token.execute(&value) {
            Ok(result) => result,
            Err(err) => panic!("{}", err),
        };

        assert!(result.is_some());
    }

    #[test]
    fn test_condition_not_between() {
        let where_token = Where::condition(Operator::NotBetween, "age", Value::from(vec![18, 20]));

        let value = Value::from(vec![("age", 19)]);
        let result = match where_token.execute(&value) {
            Ok(result) => result,
            Err(err) => panic!("{}", err),
        };

        assert!(result.is_none());
    }

    #[test]
    fn test_condition_is_null() {
        let where_token = Where::condition(Operator::IsNull, "age", Value::from(vec![18, 20]));

        let value = Value::from(vec![("age", 19)]);
        let result = match where_token.execute(&value) {
            Ok(result) => result,
            Err(err) => panic!("{}", err),
        };

        assert!(result.is_none());
    }

    #[test]
    fn test_condition_is_not_null() {
        let where_token = Where::condition(Operator::IsNotNull, "age", Value::from(vec![18, 20]));

        let value = Value::from(vec![("age", 19)]);
        let result = match where_token.execute(&value) {
            Ok(result) => result,
            Err(err) => panic!("{}", err),
        };

        assert!(result.is_some());
    }

    #[test]
    fn test_condition_regex() {
        let where_token = Where::condition(Operator::Regex, "name", sql_string!("J.*"));

        let value = Value::from(vec![("name", "John")]);
        let result = match where_token.execute(&value) {
            Ok(result) => result,
            Err(err) => panic!("{}", err),
        };

        assert_eq!(result, Some(Value::from(vec![("name", "John")])));
    }

    #[test]
    fn test_condition_not_regex() {
        let where_token = Where::condition(Operator::NotRegex, "name", sql_string!("J.*"));

        let value = Value::from(vec![("name", "John")]);
        let result = match where_token.execute(&value) {
            Ok(result) => result,
            Err(err) => panic!("{}", err),
        };

        assert_eq!(result, None);
    }
}
