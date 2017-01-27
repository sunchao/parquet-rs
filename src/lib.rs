#![feature(field_init_shorthand)]

#![allow(dead_code)]
#![allow(non_camel_case_types)]

#[macro_use]
extern crate quick_error;

#[macro_use]
pub mod errors;

pub mod basic;

pub mod schema;
pub mod file;

