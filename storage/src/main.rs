// mod repository;
// mod storage;
// use storage::s3::S3;
// use storage::storage::Storage;
// use valu3::{prelude::*, vec_value};
// use valu3_parquet::Table;
// enum Key {
//     Name,
//     Age,
//     IsStudent,
// }

// impl Into<&str> for Key {
//     fn into(self) -> &'static str {
//         match self {
//             Key::Name => "name",
//             Key::Age => "age",
//             Key::IsStudent => "is_student",
//         }
//     }
// }

// #[tokio::main]
// async fn main() {
//     let s3 = S3::builder("my-bucket".into()).await;

//     let mut table = Table::new();

//     table.add(Key::Name.into(), vec_value!["Alice", "Bob", "Charlie"]);
//     table.add(Key::Age.into(), vec_value![25, 30, 35]);
//     table.add(Key::IsStudent.into(), vec_value![true, false, true]);

//     table.load().unwrap();
// }
