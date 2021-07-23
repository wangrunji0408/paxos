use actix::prelude::*;

pub struct Learner {
    count: usize,
}

impl Actor for Learner {
    type Context = Context<Self>;
}
