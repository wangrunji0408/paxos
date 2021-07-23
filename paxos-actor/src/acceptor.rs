use actix::prelude::*;
use log::*;
use std::fmt;

pub struct Acceptor {
    id: usize,
    min_proposal: ProposalId,
    accepted_proposal: ProposalId,
    accepted_value: Option<usize>,
}

impl Actor for Acceptor {
    type Context = Context<Self>;
}

impl fmt::Debug for Acceptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Acceptor({})", self.id)
    }
}

impl Acceptor {
    pub fn new(id: usize) -> Self {
        Acceptor {
            id,
            min_proposal: ProposalId::default(),
            accepted_proposal: ProposalId::default(),
            accepted_value: None,
        }
    }
}

#[derive(Default, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct ProposalId {
    pub timestamp: usize,
    pub server_id: usize,
}

impl fmt::Debug for ProposalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({},{})", self.timestamp, self.server_id)
    }
}

#[derive(Message, Debug, Clone)]
#[rtype(result = "PrepareResponse")]
pub struct Prepare {
    pub id: ProposalId,
}

#[derive(MessageResponse)]
pub struct PrepareResponse {
    pub id: usize,
    pub accepted_proposal: ProposalId,
    pub accepted_value: Option<usize>,
}

impl Handler<Prepare> for Acceptor {
    type Result = PrepareResponse;

    fn handle(&mut self, msg: Prepare, _ctx: &mut Self::Context) -> Self::Result {
        if msg.id > self.min_proposal {
            info!("{:?}: <- {:?}", self, msg);
            self.min_proposal = msg.id;
        } else {
            info!("{:?}: x- {:?}", self, msg);
        }
        // TODO: persist
        PrepareResponse {
            id: self.id,
            accepted_proposal: self.accepted_proposal,
            accepted_value: self.accepted_value,
        }
    }
}

#[derive(Message, Debug, Clone)]
#[rtype(result = "ProposeResponse")]
pub struct Propose {
    pub id: ProposalId,
    pub value: usize,
}

#[derive(MessageResponse)]
pub struct ProposeResponse {
    pub id: usize,
    pub min_proposal: ProposalId,
}

impl Handler<Propose> for Acceptor {
    type Result = ProposeResponse;

    fn handle(&mut self, msg: Propose, _ctx: &mut Self::Context) -> Self::Result {
        if msg.id >= self.min_proposal {
            info!("{:?}: <- {:?}", self, msg);
            self.min_proposal = msg.id;
            self.accepted_proposal = msg.id;
            self.accepted_value = Some(msg.value);
        } else {
            info!("{:?}: x- {:?}", self, msg);
        }
        // TODO: persist
        ProposeResponse {
            id: self.id,
            min_proposal: self.min_proposal,
        }
    }
}
