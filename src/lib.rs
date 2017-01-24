#![feature(field_init_shorthand)]
#![allow(non_camel_case_types)]

#[macro_use]
extern crate quick_error;

pub mod basic;

pub mod errors;
pub mod schema;
pub mod file;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
