// https://medium.com/swlh/machine-learning-in-rust-linear-regression-edef3fb65f93

use nalgebra::{DMatrix, Scalar};

use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::str::FromStr;

fn parse_value<N: FromStr + Scalar + Default>(s: &str) -> N {
    if s == "NA" {
        N::default()
    } else {
        N::from_str(s).unwrap_or_else(|_| N::default())
    }
}

fn parse_csv<N, R>(input: R) -> Result<DMatrix<N>, Box<dyn std::error::Error>>
where
    N: FromStr + Scalar + Default,
    N::Err: std::error::Error,
    R: BufRead,
{
    let mut data: Vec<N> = Vec::new();
    let mut rows: usize = 0;

    for line in input.lines() {
        rows += 1;

        for datum in line?.split_terminator(",") {
            data.push(parse_value::<N>(datum.trim()));
        }
    }

    let cols: usize = data.len() / rows;

    Ok(DMatrix::from_row_slice(rows, cols, &data[..]))
}

fn main() {
    let file = File::open("HousingData.csv").unwrap();
    let bos: DMatrix<f64> = parse_csv(BufReader::new(file)).unwrap();

    println!("{}", bos.rows(0, 5));
}
