use std::collections::{BTreeMap, BTreeSet};

use dbt_schemas::schemas::telemetry::NodeType;
use dbt_schemas::schemas::{InternalDbtNode, Nodes};

/// Build a dependency map from Nodes, filtered to selected node IDs.
///
/// When a node depends on an ephemeral model (not in the selected set), walk through
/// the ephemeral model's dependencies transitively to find non-ephemeral ancestors.
pub fn build_dependency_map(
    nodes: &Nodes,
    selected_ids: &[String],
) -> BTreeMap<String, BTreeSet<String>> {
    let selected_set: BTreeSet<&str> = selected_ids.iter().map(String::as_str).collect();
    let mut deps: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for id in selected_ids {
        let mut node_deps = BTreeSet::new();
        if let Some(node) = nodes.get_node(id) {
            for (dep_id, _) in &node.base().depends_on.nodes_with_ref_location {
                if selected_set.contains(dep_id.as_str()) {
                    node_deps.insert(dep_id.clone());
                } else {
                    // Dependency is not selected (likely ephemeral) — promote its
                    // transitive dependencies so the DAG ordering is correct.
                    promote_ephemeral_deps(nodes, dep_id, &selected_set, &mut node_deps);
                }
            }
        }
        deps.insert(id.clone(), node_deps);
    }

    deps
}

/// Walk through an unselected (ephemeral) node's dependencies transitively,
/// collecting any that ARE in the selected set.
fn promote_ephemeral_deps(
    nodes: &Nodes,
    unselected_id: &str,
    selected_set: &BTreeSet<&str>,
    result: &mut BTreeSet<String>,
) {
    let mut stack = vec![unselected_id.to_string()];
    let mut visited = BTreeSet::new();
    while let Some(current) = stack.pop() {
        if !visited.insert(current.clone()) {
            continue;
        }
        if let Some(node) = nodes.get_node(&current) {
            for (dep_id, _) in &node.base().depends_on.nodes_with_ref_location {
                if selected_set.contains(dep_id.as_str()) {
                    result.insert(dep_id.clone());
                } else if !visited.contains(dep_id.as_str()) {
                    stack.push(dep_id.clone());
                }
            }
        }
    }
}

/// Inject test-gate dependencies, using the rule of `dbt build`.
///
/// A data test T runs before the models and snapshots that consume what it
/// tests, so a failing test skips them. For T with upstream nodes P(T) (whatever
/// it references: models, seeds, snapshots or sources), the edge `N → T` is added
/// for a model or snapshot N when
///
/// * N directly depends on at least one node of P(T),
/// * N is not itself an ancestor of T (T already runs after N), and
/// * **every** node of P(T) is an ancestor of N.
///
/// The last condition is what keeps the added edge from closing a cycle: any path
/// from T to N would have to leave T through a node of P(T) that is not upstream
/// of N. It also means a test spanning several parents (a `relationships` test)
/// gates only what is downstream of all of them.
///
/// Unit tests do not take part: they run before the model they test and are
/// wired by [`inject_unit_test_gates`]. Seeds, sources and tests are never gated.
///
/// Ancestors are read from the dependency graph as passed in, so gate edges added
/// here never influence which later gates are skipped, and the result does not
/// depend on iteration order.
///
/// # Complexity
///
/// With V nodes and E dependency edges in `deps`, and P (node, test) gate
/// candidates, the transitive ancestors of every test, model and snapshot come
/// from one strongly-connected-component pass plus one reachability-bitset union
/// per component, so they cost O((V + E) * V / 64) time and O(V^2 / 64) bits in
/// the worst case. Each candidate is then checked with one O(log V) membership
/// test for the cycle guard and one per parent of its test. Computing a separate
/// ancestor set per node by graph traversal would instead cost O(V * (V + E)),
/// which grows quadratically on projects whose test count scales with their
/// model count.
pub fn inject_test_gates(
    nodes: &Nodes,
    selected_ids: &[String],
    deps: &mut BTreeMap<String, BTreeSet<String>>,
) {
    // Tests are the gate sources; models and snapshots are the nodes that can be
    // gated. Every one of them needs ancestor queries, so all are index roots.
    let mut tests: Vec<&str> = Vec::new();
    let mut roots: Vec<&str> = Vec::new();
    // Gateable nodes by the node they directly depend on.
    let mut consumers: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for id in selected_ids {
        match nodes.get_node(id).map(InternalDbtNode::resource_type) {
            Some(NodeType::Test) => {
                tests.push(id);
                roots.push(id);
            }
            Some(NodeType::Model | NodeType::Snapshot) => {
                roots.push(id);
                for dep in deps.get(id).into_iter().flatten() {
                    consumers.entry(dep).or_default().push(id);
                }
            }
            _ => {}
        }
    }

    let ancestors = AncestorIndex::new(deps, &roots);
    let mut gates: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for test in tests {
        let Some(parents) = deps.get(test) else {
            continue;
        };
        let mut considered: BTreeSet<&str> = BTreeSet::new();
        for parent in parents {
            for &node in consumers.get(parent.as_str()).into_iter().flatten() {
                // A node reachable from several parents is judged once.
                if !considered.insert(node) || ancestors.is_ancestor(test, node) {
                    continue;
                }
                if ancestors.has_all_ancestors(node, parents) {
                    gates
                        .entry(node.to_string())
                        .or_default()
                        .insert(test.to_string());
                }
            }
        }
    }

    for (node, node_gates) in gates {
        if let Some(entry) = deps.get_mut(&node) {
            entry.extend(node_gates);
        }
    }
}

/// Reachability over the raw dependency graph: answers "is `node` a transitive
/// dependency of `root`" (and whether all of several nodes are) for a fixed set of
/// roots.
///
/// Strongly-connected components are condensed first so that the answer is also exact
/// for a graph that contains a cycle (a node on a cycle is its own ancestor); the
/// reachable set of a component is the union of the sets of the components it points
/// to, which Tarjan's algorithm emits successors-first.
struct AncestorIndex {
    index_of: BTreeMap<String, usize>,
    /// Component of each root's node, keyed by node index.
    component_of: BTreeMap<usize, usize>,
    /// Per component, the bitset of nodes reachable through at least one edge.
    reachable: Vec<Vec<u64>>,
}

impl AncestorIndex {
    fn new(deps: &BTreeMap<String, BTreeSet<String>>, roots: &[&str]) -> Self {
        let mut index_of: BTreeMap<String, usize> = BTreeMap::new();
        let mut intern = |id: &str| {
            let next = index_of.len();
            *index_of.entry(id.to_string()).or_insert(next)
        };
        for (node, node_deps) in deps {
            intern(node);
            for dep in node_deps {
                intern(dep);
            }
        }
        let n = index_of.len();
        let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
        for (node, node_deps) in deps {
            adj[index_of[node]] = node_deps.iter().map(|d| index_of[d]).collect();
        }

        let words = n.div_ceil(64);
        let unvisited = usize::MAX;
        let mut order = vec![unvisited; n];
        let mut low = vec![0usize; n];
        let mut on_stack = vec![false; n];
        let mut component = vec![unvisited; n];
        let mut reachable: Vec<Vec<u64>> = Vec::new();
        let mut scc_stack: Vec<usize> = Vec::new();
        let mut counter = 0usize;

        // Iterative so a dependency chain thousands of models deep cannot
        // overflow the call stack.
        for root in roots.iter().filter_map(|r| index_of.get(*r).copied()) {
            if order[root] != unvisited {
                continue;
            }
            let mut calls: Vec<(usize, usize)> = vec![(root, 0)];
            order[root] = counter;
            low[root] = counter;
            counter += 1;
            scc_stack.push(root);
            on_stack[root] = true;
            while let Some(&(v, edge)) = calls.last() {
                if let Some(&w) = adj[v].get(edge) {
                    if let Some(frame) = calls.last_mut() {
                        frame.1 += 1;
                    }
                    if order[w] == unvisited {
                        order[w] = counter;
                        low[w] = counter;
                        counter += 1;
                        scc_stack.push(w);
                        on_stack[w] = true;
                        calls.push((w, 0));
                    } else if on_stack[w] {
                        low[v] = low[v].min(order[w]);
                    }
                    continue;
                }
                calls.pop();
                if let Some(&(parent, _)) = calls.last() {
                    low[parent] = low[parent].min(low[v]);
                }
                if low[v] != order[v] {
                    continue;
                }
                let id = reachable.len();
                let mut members = Vec::new();
                while let Some(m) = scc_stack.pop() {
                    on_stack[m] = false;
                    component[m] = id;
                    members.push(m);
                    if m == v {
                        break;
                    }
                }
                let mut bits = vec![0u64; words];
                // Members of a cycle reach one another (and themselves).
                if members.len() > 1 || adj[v].contains(&v) {
                    for &m in &members {
                        bits[m / 64] |= 1 << (m % 64);
                    }
                }
                for &m in &members {
                    for &w in &adj[m] {
                        if component[w] == id {
                            continue;
                        }
                        bits[w / 64] |= 1 << (w % 64);
                        // `w`'s component closed earlier: it is not on the stack.
                        for (dst, src) in bits.iter_mut().zip(&reachable[component[w]]) {
                            *dst |= src;
                        }
                    }
                }
                reachable.push(bits);
            }
        }

        let component_of = roots
            .iter()
            .filter_map(|r| index_of.get(*r).map(|&i| (i, component[i])))
            .collect();
        Self {
            index_of,
            component_of,
            reachable,
        }
    }

    /// Whether `node` is reachable from `root` through one or more dependency
    /// edges. `root` must have been passed to [`Self::new`].
    fn is_ancestor(&self, root: &str, node: &str) -> bool {
        let (Some(&r), Some(&n)) = (self.index_of.get(root), self.index_of.get(node)) else {
            return false;
        };
        self.component_of
            .get(&r)
            .is_some_and(|&c| self.reachable[c][n / 64] & (1 << (n % 64)) != 0)
    }

    /// Whether every one of `nodes` is reachable from `root`; vacuously true for
    /// an empty set. `root` must have been passed to [`Self::new`].
    fn has_all_ancestors<'a>(
        &self,
        root: &str,
        nodes: impl IntoIterator<Item = &'a String>,
    ) -> bool {
        let reachable = self
            .index_of
            .get(root)
            .and_then(|r| self.component_of.get(r))
            .map(|&c| &self.reachable[c]);
        nodes.into_iter().all(|node| {
            reachable.is_some_and(|bits| {
                self.index_of
                    .get(node)
                    .is_some_and(|&n| bits[n / 64] & (1 << (n % 64)) != 0)
            })
        })
    }
}

/// Rewire unit test edges so unit tests run BEFORE the model they test.
///
/// Matches dbt semantics: a unit test executes the model's SQL against
/// fixtures, so it needs the model's *inputs* (for column-type probing of
/// mocked relations) but not the model itself; the model in turn must wait
/// for its unit tests and is skipped when one fails.
///
/// Concretely, for unit test U testing model M:
/// * `deps[U]` loses M and inherits M's own selected deps.
/// * `deps[M]` gains U (only when M is selected — `dbt test`-style runs
///   schedule unit tests without their models).
pub fn inject_unit_test_gates(
    nodes: &Nodes,
    selected_ids: &[String],
    deps: &mut BTreeMap<String, BTreeSet<String>>,
) {
    let selected_set: BTreeSet<&str> = selected_ids.iter().map(String::as_str).collect();

    // Snapshot tested-model deps first so two unit tests on the same model
    // both inherit the model's ORIGINAL inputs (not each other via the gate
    // edges added below).
    let mut rewires: Vec<(String, String, BTreeSet<String>)> = Vec::new();
    for id in selected_ids {
        let Some(unit) = nodes.unit_tests.get(id) else {
            continue;
        };
        let Some(model_id) = unit
            .tested_node_unique_id
            .clone()
            .or_else(|| unit.__base_attr__.depends_on.nodes.first().cloned())
        else {
            continue;
        };
        let model_deps = deps.get(&model_id).cloned().unwrap_or_default();
        rewires.push((id.clone(), model_id, model_deps));
    }

    for (unit_id, model_id, model_deps) in rewires {
        if let Some(unit_deps) = deps.get_mut(&unit_id) {
            unit_deps.remove(&model_id);
            unit_deps.extend(model_deps);
        }
        if selected_set.contains(model_id.as_str())
            && let Some(m_deps) = deps.get_mut(&model_id)
        {
            m_deps.insert(unit_id);
        }
    }
}

/// A dependency map in which no node waits for any other.
///
/// The freshness commands measure relations that already exist; they neither
/// build anything nor read a node's upstream, so the project DAG carries no
/// meaning for them. Reusing [`build_dependency_map`] would be actively wrong:
/// a model with an SLA that reads a freshness-checked source would wait on it,
/// and a *stale* source would then skip the model instead of measuring it —
/// dbt reports a status for every node the command selected.
pub fn independent_nodes(selected_ids: &[String]) -> BTreeMap<String, BTreeSet<String>> {
    selected_ids
        .iter()
        .map(|id| (id.clone(), BTreeSet::new()))
        .collect()
}

/// Compute topological levels from a dependency map using Kahn's algorithm.
/// Returns levels where each level's nodes have no deps on nodes in the same or later levels.
pub fn topological_levels(
    deps: &BTreeMap<String, BTreeSet<String>>,
) -> Result<Vec<Vec<String>>, anyhow::Error> {
    let mut in_degree: BTreeMap<&str, usize> = BTreeMap::new();
    let mut reverse: BTreeMap<&str, Vec<&str>> = BTreeMap::new();

    for (node, node_deps) in deps {
        let node_str = node.as_str();
        // Ensures source nodes (no deps) land in the map with in-degree 0
        // so they end up in the initial queue.
        *in_degree.entry(node_str).or_insert(0) += node_deps.len();
        for dep in node_deps {
            in_degree.entry(dep.as_str()).or_insert(0);
            reverse.entry(dep.as_str()).or_default().push(node_str);
        }
    }

    let mut levels = Vec::new();
    let mut queue: Vec<&str> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(&node, _)| node)
        .collect();
    queue.sort_unstable(); // deterministic ordering

    while !queue.is_empty() {
        levels.push(queue.iter().map(ToString::to_string).collect());
        let mut next_queue = Vec::new();
        for &node in &queue {
            if let Some(dependents) = reverse.get(node) {
                for &dep in dependents {
                    if let Some(deg) = in_degree.get_mut(dep) {
                        *deg -= 1;
                        if *deg == 0 {
                            next_queue.push(dep);
                        }
                    }
                }
            }
        }
        next_queue.sort_unstable();
        queue = next_queue;
    }

    // Verify all nodes were processed — if not, the graph contains a cycle.
    let processed: usize = levels.iter().map(Vec::len).sum();
    if processed != in_degree.len() {
        let stuck: Vec<&str> = in_degree
            .iter()
            .filter(|(_, deg)| **deg > 0)
            .map(|(&node, _)| node)
            .collect();
        anyhow::bail!(
            "cyclic dependency detected: {} node(s) not reachable: {}",
            stuck.len(),
            stuck.join(", ")
        );
    }

    Ok(levels)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
