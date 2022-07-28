use super::PushBuild;

use std::marker::PhantomData;

use crate::scheduled::context::Context;
use crate::scheduled::handoff::handoff_list::PortList;
use crate::scheduled::port::SEND;

use pusherator::filter_map::FilterMap;

pub struct FilterMapPushBuild<Next, Func, In>
where
    Next: PushBuild,
    Func: FnMut(&Context, In) -> Option<Next::ItemIn>,
{
    next: Next,
    func: Func,
    _phantom: PhantomData<fn(In)>,
}
impl<Next, Func, In> FilterMapPushBuild<Next, Func, In>
where
    Next: PushBuild,
    Func: FnMut(&Context, In) -> Option<Next::ItemIn>,
{
    pub fn new(next: Next, func: Func) -> Self {
        Self {
            next,
            func,
            _phantom: PhantomData,
        }
    }
}

#[allow(type_alias_bounds)]
type PushBuildImpl<'slf, 'ctx, Next, Func, In>
where
    Next: PushBuild,
    Func: 'slf + FnMut(&Context, In) -> Option<Next::ItemIn>,
= FilterMap<Next::Build<'slf, 'ctx>, impl FnMut(In) -> Option<Next::ItemIn>, In>;

impl<Next, Func, In> PushBuild for FilterMapPushBuild<Next, Func, In>
where
    Next: PushBuild,
    Func: FnMut(&Context, In) -> Option<Next::ItemIn>,
{
    type ItemIn = In;
    type Build<'slf, 'ctx> = PushBuildImpl<'slf, 'ctx, Next, Func, In>
    where
        Self: 'slf;

    type OutputHandoffs = Next::OutputHandoffs;

    fn build<'slf, 'ctx>(
        &'slf mut self,
        context: &'ctx Context,
        handoffs: <Self::OutputHandoffs as PortList<SEND>>::Ctx<'ctx>,
    ) -> Self::Build<'slf, 'ctx> {
        FilterMap::new(
            |x| (self.func)(context, x),
            self.next.build(context, handoffs),
        )
    }
}
