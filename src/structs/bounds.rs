use std::ops::Div;

#[derive(Debug, Clone)]
pub struct Bounds {
    pub max: i64,
    pub min: i64,
}

impl Bounds {
    pub fn get_iterations(&self, batch_size: &i64) -> i64 {
        let raw: i64 = ((self.max - self.min).div(batch_size) as f64).floor() as i64;
        let retval: i64 = if raw == 0 { 1 } else { raw };
        return retval;
    }
}
