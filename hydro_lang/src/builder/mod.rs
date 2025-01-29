use std::any::type_name;
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::rc::Rc;

#[cfg(feature = "build")]
use compiled::CompiledFlow;
#[cfg(feature = "build")]
use deploy::{DeployFlow, DeployResult};
use stageleft::*;

#[cfg(feature = "build")]
use crate::deploy::{ClusterSpec, Deploy, ExternalSpec, IntoProcessSpec, LocalDeploy};
use crate::ir::HydroLeaf;
use crate::location::{Cluster, ExternalProcess, Process};
use crate::staging_util::Invariant;

#[cfg(feature = "build")]
pub mod built;
#[cfg(feature = "build")]
pub mod compiled;
#[cfg(feature = "build")]
pub mod deploy;

pub struct FlowStateInner {
    /// Tracks the leaves of the dataflow IR. This is referenced by
    /// `Stream` and `HfCycle` to build the IR. The inner option will
    /// be set to `None` when this builder is finalized.
    pub(crate) leaves: Option<Vec<HydroLeaf>>,

    /// Counter for generating unique external output identifiers.
    pub(crate) next_external_out: usize,

    /// Counters for generating identifiers for cycles.
    pub(crate) cycle_counts: HashMap<usize, usize>,

    /// Counters for clock IDs.
    pub(crate) next_clock_id: usize,
}

pub type FlowState = Rc<RefCell<FlowStateInner>>;

pub const FLOW_USED_MESSAGE: &str = "Attempted to add a leaf to a flow that has already been finalized. No leaves can be added after the flow has been compiled.";

pub struct FlowBuilder<'a> {
    flow_state: FlowState,
    processes: RefCell<Vec<(usize, String)>>,
    clusters: RefCell<Vec<(usize, String)>>,
    externals: RefCell<Vec<(usize, String)>>,

    next_node_id: RefCell<usize>,

    /// Tracks whether this flow has been finalized; it is an error to
    /// drop without finalizing.
    finalized: bool,

    /// 'a on a FlowBuilder is used to ensure that staged code does not
    /// capture more data that it is allowed to; 'a is generated at the
    /// entrypoint of the staged code and we keep it invariant here
    /// to enforce the appropriate constraints
    _phantom: Invariant<'a>,
}

impl Drop for FlowBuilder<'_> {
    fn drop(&mut self) {
        if !self.finalized {
            panic!("Dropped FlowBuilder without finalizing, you may have forgotten to call `with_default_optimize`, `optimize_with`, or `finalize`.");
        }
    }
}

impl QuotedContext for FlowBuilder<'_> {
    fn create() -> Self {
        FlowBuilder::new()
    }
}

impl<'a> FlowBuilder<'a> {
    #[expect(
        clippy::new_without_default,
        reason = "call `new` explicitly, not `default`"
    )]
    pub fn new() -> FlowBuilder<'a> {
        FlowBuilder {
            flow_state: Rc::new(RefCell::new(FlowStateInner {
                leaves: Some(vec![]),
                next_external_out: 0,
                cycle_counts: HashMap::new(),
                next_clock_id: 0,
            })),
            processes: RefCell::new(vec![]),
            clusters: RefCell::new(vec![]),
            externals: RefCell::new(vec![]),
            next_node_id: RefCell::new(0),
            finalized: false,
            _phantom: PhantomData,
        }
    }

    #[cfg(feature = "build")]
    pub fn finalize(mut self) -> built::BuiltFlow<'a> {
        self.finalized = true;

        built::BuiltFlow {
            ir: self.flow_state.borrow_mut().leaves.take().unwrap(),
            process_id_name: self.processes.replace(vec![]),
            cluster_id_name: self.clusters.replace(vec![]),
            external_id_name: self.externals.replace(vec![]),
            used: false,
            _phantom: PhantomData,
        }
    }

    #[cfg(feature = "build")]
    pub fn with_default_optimize<D: LocalDeploy<'a>>(self) -> DeployFlow<'a, D> {
        self.finalize().with_default_optimize()
    }

    #[cfg(feature = "build")]
    pub fn optimize_with(
        self,
        f: impl FnOnce(Vec<HydroLeaf>) -> Vec<HydroLeaf>,
    ) -> built::BuiltFlow<'a> {
        self.finalize().optimize_with(f)
    }

    pub fn flow_state(&self) -> &FlowState {
        &self.flow_state
    }

    pub fn process<P>(&self) -> Process<'a, P> {
        let mut next_node_id = self.next_node_id.borrow_mut();
        let id = *next_node_id;
        *next_node_id += 1;

        self.processes
            .borrow_mut()
            .push((id, type_name::<P>().to_string()));

        Process {
            id,
            flow_state: self.flow_state().clone(),
            _phantom: PhantomData,
        }
    }

    pub fn external_process<P>(&self) -> ExternalProcess<'a, P> {
        let mut next_node_id = self.next_node_id.borrow_mut();
        let id = *next_node_id;
        *next_node_id += 1;

        self.externals
            .borrow_mut()
            .push((id, type_name::<P>().to_string()));

        ExternalProcess {
            id,
            flow_state: self.flow_state().clone(),
            _phantom: PhantomData,
        }
    }

    pub fn cluster<C>(&self) -> Cluster<'a, C> {
        let mut next_node_id = self.next_node_id.borrow_mut();
        let id = *next_node_id;
        *next_node_id += 1;

        self.clusters
            .borrow_mut()
            .push((id, type_name::<C>().to_string()));

        Cluster {
            id,
            flow_state: self.flow_state().clone(),
            _phantom: PhantomData,
        }
    }

    #[cfg(feature = "build")]
    pub fn with_process<P, D: LocalDeploy<'a>>(
        self,
        process: &Process<P>,
        spec: impl IntoProcessSpec<'a, D>,
    ) -> DeployFlow<'a, D> {
        self.with_default_optimize().with_process(process, spec)
    }

    #[cfg(feature = "build")]
    pub fn with_remaining_processes<D: LocalDeploy<'a>, S: IntoProcessSpec<'a, D> + 'a>(
        self,
        spec: impl Fn() -> S,
    ) -> DeployFlow<'a, D> {
        self.with_default_optimize().with_remaining_processes(spec)
    }

    #[cfg(feature = "build")]
    pub fn with_external<P, D: LocalDeploy<'a>>(
        self,
        process: &ExternalProcess<P>,
        spec: impl ExternalSpec<'a, D>,
    ) -> DeployFlow<'a, D> {
        self.with_default_optimize().with_external(process, spec)
    }

    #[cfg(feature = "build")]
    pub fn with_remaining_externals<D: LocalDeploy<'a>, S: ExternalSpec<'a, D> + 'a>(
        self,
        spec: impl Fn() -> S,
    ) -> DeployFlow<'a, D> {
        self.with_default_optimize().with_remaining_externals(spec)
    }

    #[cfg(feature = "build")]
    pub fn with_cluster<C, D: LocalDeploy<'a>>(
        self,
        cluster: &Cluster<C>,
        spec: impl ClusterSpec<'a, D>,
    ) -> DeployFlow<'a, D> {
        self.with_default_optimize().with_cluster(cluster, spec)
    }

    #[cfg(feature = "build")]
    pub fn with_remaining_clusters<D: LocalDeploy<'a>, S: ClusterSpec<'a, D> + 'a>(
        self,
        spec: impl Fn() -> S,
    ) -> DeployFlow<'a, D> {
        self.with_default_optimize().with_remaining_clusters(spec)
    }

    #[cfg(feature = "build")]
    pub fn compile<D: Deploy<'a>>(self, env: &D::CompileEnv) -> CompiledFlow<'a, D::GraphId> {
        self.with_default_optimize::<D>().compile(env)
    }

    #[cfg(feature = "build")]
    pub fn compile_no_network<D: LocalDeploy<'a>>(self) -> CompiledFlow<'a, D::GraphId> {
        self.with_default_optimize::<D>().compile_no_network()
    }

    #[cfg(feature = "build")]
    pub fn deploy<D: Deploy<'a, CompileEnv = ()>>(
        self,
        env: &mut D::InstantiateEnv,
    ) -> DeployResult<'a, D> {
        self.with_default_optimize().deploy(env)
    }
}
