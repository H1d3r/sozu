pub trait Aggregator {}

#[derive(Debug, Clone)]
pub struct SimpleAggregator {
    pub received: usize,
    pub sent: usize,
}
impl Aggregator for SimpleAggregator {}
