use super::acceptor::*;
use actix::prelude::*;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use log::*;
use std::fmt;

pub struct Proposer {
    id: usize,
    acceptors: Vec<Addr<Acceptor>>,
}

impl Actor for Proposer {
    type Context = Context<Self>;
}

impl Proposer {
    pub fn new(id: usize, acceptors: Vec<Addr<Acceptor>>) -> Proposer {
        Proposer { id, acceptors }
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
        self.handle(
            Propose1 {
                timestamp: 1,
                value: msg.value,
            },
            ctx,
        )
    }
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
struct Propose1 {
    timestamp: usize,
    value: usize,
}

impl Handler<Propose1> for Proposer {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, msg: Propose1, ctx: &mut Self::Context) -> Self::Result {
        let prepare = Prepare {
            id: ProposalId {
                timestamp: msg.timestamp,
                server_id: self.id,
            },
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
            let mut max_accepted_proposal = ProposalId::default();
            let mut max_accepted_value = None;
            while let Some(rsp) = responses.next().await {
                if let Ok(rsp) = rsp {
                    info!(
                        "Proposer({}): <-{} {{ id: {:?}, value: {:?} }}",
                        id, rsp.id, rsp.accepted_proposal, rsp.accepted_value
                    );
                    if !rsp.ok {
                        continue;
                    }
                    if rsp.accepted_proposal > max_accepted_proposal {
                        max_accepted_proposal = rsp.accepted_proposal;
                        max_accepted_value = rsp.accepted_value;
                    }
                    count += 1;
                    if count >= majority {
                        addr.try_send(Propose2 {
                            id: prepare.id,
                            value: max_accepted_value.unwrap_or(msg.value),
                        })
                        .unwrap();
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
    value: usize,
}

impl Handler<Propose2> for Proposer {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, msg: Propose2, ctx: &mut Self::Context) -> Self::Result {
        let propose = Propose {
            id: msg.id,
            value: msg.value,
        };
        info!("{:?}: => {:?}", self, propose);
        let mut responses = self
            .acceptors
            .iter()
            .map(|acceptor| acceptor.send(propose.clone()))
            .collect::<FuturesUnordered<_>>();
        let majority = (responses.len() + 1) / 2;
        let addr = ctx.address();
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
                        addr.try_send(Propose1 {
                            timestamp: rsp.min_proposal.timestamp + 1,
                            value: propose.value,
                        })
                        .unwrap();
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
