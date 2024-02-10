use valu3::{prelude::*, vec_value};
use valu3_parquet::Table;

enum Key {
    Name,
    Age,
    IsStudent,
}

impl Into<&str> for Key {
    fn into(self) -> &'static str {
        match self {
            Key::Name => "name",
            Key::Age => "age",
            Key::IsStudent => "is_student",
        }
    }
}

fn main() {
    let mut table = Table::new();

    table.add(Key::Name.into(), vec_value!["Alice", "Bob", "Charlie"]);
    table.add(Key::Age.into(), vec_value![25, 30, 35]);
    table.add(Key::IsStudent.into(), vec_value![true, false, true]);

    table.load().unwrap();

    table.to_parquet("example.parquet").unwrap();

    let table = Table::from_parquet("example.parquet").unwrap();

    let name: &Vec<Value> = table.get(Key::Name.into()).unwrap();
    let age = table.get(Key::Age.into()).unwrap();
    let is_student = table.get(Key::IsStudent.into()).unwrap();

    assert_eq!(name, &vec_value!["Alice", "Bob", "Charlie"]);
    assert_eq!(age, &vec_value![25, 30, 35]);
    assert_eq!(is_student, &vec_value![true, false, true]);
}
