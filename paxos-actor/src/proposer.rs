use super::acceptor::*;
use actix::prelude::*;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use log::*;
use std::fmt;

pub struct Proposer {
    id: usize,
    proposal_id: ProposalId,
    value: Option<usize>,
    acceptors: Vec<Addr<Acceptor>>,
}

impl Actor for Proposer {
    type Context = Context<Self>;
}

impl Proposer {
    pub fn new(id: usize, acceptors: Vec<Addr<Acceptor>>) -> Proposer {
        Proposer {
            id,
            proposal_id: ProposalId::default(),
            value: None,
            acceptors,
        }
    }
}

impl fmt::Debug for Proposer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Proposer({})", self.id)
    }
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct ProposeRequest {
    pub value: usize,
}

impl Handler<ProposeRequest> for Proposer {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, msg: ProposeRequest, ctx: &mut Self::Context) -> Self::Result {
        self.proposal_id.server_id = self.id;
        self.proposal_id.timestamp += 1;
        self.value = Some(msg.value);
        let prepare = Prepare {
            id: self.proposal_id,
        };
        info!("{:?}: => {:?}", self, prepare);
        let mut responses = self
            .acceptors
            .iter()
            .map(|acceptor| acceptor.send(prepare.clone()))
            .collect::<FuturesUnordered<_>>();
        let majority = (responses.len() + 1) / 2;
        let addr = ctx.address();
        let id = self.id;
        async move {
            let mut count = 0;
            let mut propose = Propose2::default();
            while let Some(rsp) = responses.next().await {
                if let Ok(rsp) = rsp {
                    info!(
                        "Proposer({}): <-{} {{ id: {:?}, value: {:?} }}",
                        id, rsp.id, rsp.accepted_proposal, rsp.accepted_value
                    );
                    if rsp.accepted_proposal > propose.id {
                        propose.id = rsp.accepted_proposal;
                        propose.value = rsp.accepted_value;
                    }
                    count += 1;
                    if count >= majority {
                        addr.try_send(propose).unwrap();
                        return;
                    }
                } else if let Err(e) = rsp {
                    error!("Proposer({}): <-? {:?}", id, e);
                }
            }
        }
        .boxed_local()
    }
}

#[derive(Message, Debug, Default)]
#[rtype(result = "()")]
struct Propose2 {
    id: ProposalId,
    value: Option<usize>,
}

impl Handler<Propose2> for Proposer {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, msg: Propose2, _ctx: &mut Self::Context) -> Self::Result {
        if msg.value.is_some() {
            self.proposal_id = msg.id;
            self.value = msg.value;
        }
        let propose = Propose {
            id: self.proposal_id,
            value: self.value.unwrap(),
        };
        info!("{:?}: => {:?}", self, propose);
        let mut responses = self
            .acceptors
            .iter()
            .map(|acceptor| acceptor.send(propose.clone()))
            .collect::<FuturesUnordered<_>>();
        let majority = (responses.len() + 1) / 2;
        let id = self.id;
        async move {
            let mut count = 0;
            while let Some(rsp) = responses.next().await {
                if let Ok(rsp) = rsp {
                    info!(
                        "Proposer({}): <-{} {{ id: {:?} }}",
                        id, rsp.id, rsp.min_proposal
                    );
                    if rsp.min_proposal > propose.id {
                        todo!("retry");
                        return;
                    }
                    count += 1;
                    if count >= majority {
                        info!("Proposer({}): chosen value={}", id, propose.value);
                        return;
                    }
                } else if let Err(e) = rsp {
                    error!("Proposer({}): <-? {:?}", id, e);
                }
            }
        }
        .boxed_local()
    }
}
