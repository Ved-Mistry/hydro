use super::{PullBuild, PullBuildBase};

use std::marker::PhantomData;

use crate::scheduled::context::Context;
use crate::scheduled::handoff::handoff_list::PortList;
use crate::scheduled::handoff::Handoff;
use crate::scheduled::port::{RecvPort, RECV};
use crate::{tl, tt};

pub struct HandoffPullBuild<Hof>
where
    Hof: Handoff,
{
    _phantom: PhantomData<fn(Hof)>,
}

impl<Hof> Default for HandoffPullBuild<Hof>
where
    Hof: Handoff,
{
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<Hof> HandoffPullBuild<Hof>
where
    Hof: Handoff,
{
    pub fn new() -> Self {
        Default::default()
    }
}

impl<Hof> PullBuildBase for HandoffPullBuild<Hof>
where
    Hof: Handoff,
{
    type ItemOut = Hof::Inner;
    type Build<'slf, 'ctx> = std::array::IntoIter<Hof::Inner, 1>;
}

impl<Hof> PullBuild for HandoffPullBuild<Hof>
where
    Hof: Handoff,
{
    type InputHandoffs = tt!(RecvPort<Hof>);

    fn build<'slf, 'ctx>(
        &'slf mut self,
        _context: &'ctx Context,
        handoffs: <Self::InputHandoffs as PortList<RECV>>::Ctx<'ctx>,
    ) -> Self::Build<'slf, 'ctx> {
        let tl!(handoff) = handoffs;
        [handoff.take_inner()].into_iter()
    }
}
