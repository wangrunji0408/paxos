mod acceptor;
mod learner;
mod proposer;

use self::acceptor::*;
use self::learner::*;
use self::proposer::*;

#[cfg(test)]
mod tests {
    use super::*;
    use actix::prelude::*;

    #[actix::test]
    async fn it_works() {
        env_logger::init();

        let acceptors: Vec<_> = (0..5).map(|id| Acceptor::new(id).start()).collect();
        let proposers: Vec<_> = (0..3)
            .map(|id| Proposer::new(id, acceptors.clone()).start())
            .collect();

        proposers[0].try_send(ProposeRequest { value: 0 }).unwrap();
        proposers[1]
            .send(ProposeRequest { value: 1 })
            .await
            .unwrap();
    }
}
