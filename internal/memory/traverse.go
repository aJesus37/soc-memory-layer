package memory

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"socmem/internal/entity"
	"socmem/internal/graph"
)

// traverseHopsMin / traverseHopsMax bound the multi-hop depth. Values
// outside the range are ERRORS, not silent clamps — API-layer defaults stay
// visible at the call site (mirrors Similar's k posture).
const (
	traverseHopsMin = 1
	traverseHopsMax = 3
)

// maxTraversePaths caps how many Paths one Traverse may return. Each
// distinct reached node contributes at least one path, so the cap bounds
// both fanout explosion and the DQL IN-list size of every hop.
const maxTraversePaths = 100

// relationRe constrains the optional relation filter to the predicate
// charset the write path accepts (`^[a-z][a-z0-9_]{0,40}$`); anything else
// can never match a stored relation and is rejected up front rather than
// silently returning an empty result.
var relationRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,40}$`)

// Path is one walked route through the projected graph: Nodes[i+1] hangs
// off Nodes[i] via Relations[i]. Nodes carries fully hydrated entities,
// start entity first.
type Path struct {
	Nodes     []entity.Entity
	Relations []string
}

// hopQueryTmpl expands one BFS level: the whole frontier is probed in ONE
// eq(ch_id, [...]) root whose sub-selections pull outgoing related_to and
// incoming ~related_to edges together with their facets (a missing @facets
// sub-selection OMITS the facet data entirely). Facet FILTERING happens in
// Go afterwards (post-filter posture, design §6): the manual hop loop —
// rather than DQL recurse — exists precisely so per-edge facet filters
// stay explicit and debuggable.
const hopQueryTmpl = `{ p(func: eq(ch_id, [%s])) { uid ch_id scope ` +
	`related_to @facets(relation, valid_from, valid_to) { uid ch_id scope } ` +
	`~related_to @facets(relation, valid_from, valid_to) { uid ch_id scope } } }`

// dgTarget is one edge endpoint as returned under related_to /
// ~related_to. Facet values ride ON THE TARGET OBJECT as sibling keys
// prefixed with the predicate name as spelled in the query; both spellings
// (with and without the reverse tilde) are decoded defensively because
// servers have varied on prefixing reverse-edge facets.
type dgTarget struct {
	Uid   string `json:"uid"`
	ChID  string `json:"ch_id"`
	Scope string `json:"scope"`

	FRel string `json:"related_to|relation"`
	FVf  string `json:"related_to|valid_from"`
	FVt  string `json:"related_to|valid_to"`

	RRel string `json:"~related_to|relation"`
	RVf  string `json:"~related_to|valid_from"`
	RVt  string `json:"~related_to|valid_to"`
}

// facets picks whichever facet spelling populated.
func (t dgTarget) facets() (rel, validFrom, validTo string) {
	if t.FRel != "" || t.FVf != "" || t.FVt != "" {
		return t.FRel, t.FVf, t.FVt
	}
	return t.RRel, t.RVf, t.RVt
}

// dgNode is one frontier node as returned by a hop query.
type dgNode struct {
	Uid   string     `json:"uid"`
	ChID  string     `json:"ch_id"`
	Scope string     `json:"scope"`
	Out   []dgTarget `json:"related_to"`
	In    []dgTarget `json:"~related_to"`
}

// walkNode is one node of the BFS tree built during graph traversal.
// parent indexes the preceding node in the same Path; rel is the relation
// that connected it.
type walkNode struct {
	uid    string
	chID   string
	parent int
	rel    string
}

// Traverse walks the projected graph outward from one entity up to hops
// deep, returning one Path per distinct reached node — start entity first
// in each path, terminal entity last, sorted by terminal entity id for a
// deterministic response. The start itself is never an endpoint, so an
// entity with no reachable neighbors yields an empty slice.
//
// Validation: scope and rawKey are REQUIRED (ErrInvalidInput otherwise);
// relation, when non-empty, must match ^[a-z][a-z0-9_]{0,40}$; hops outside
// [1,3] is an ERROR, never a silent clamp. An empty relation means "any
// relation".
//
// Miss semantics mirror Enrich: a blank-after-trim key errors, but any
// non-empty key that does not normalize — or normalizes to no stored
// entity in the scope — yields an empty slice with NO error. Reads never
// create entities.
//
// Two engines, one contract (design §6):
//
//   - Graph mode (WithGraph attached): a manual BFS hop loop over Dgraph,
//     expanding related_to and ~related_to from the start node's ch_id up
//     to hops levels. Facets are filtered post-read: relation must match
//     the filter when set, and the valid_from/valid_to window (RFC3339
//     facet strings, parsed defensively; malformed values fail CLOSED)
//     is evaluated against time.Now(). Every reached node's scope
//     predicate must equal the request scope — defense-in-depth on top of
//     the per-scope projection. A uid-keyed visited set terminates cycles;
//     parent pointers strictly descend BFS levels, so every reconstructed
//     Path is simple (no repeated node) by construction.
//   - Fallback mode (no graph store, or any Dgraph error): at most ONE hop
//     via the enrich-style mem.edges query, both directions, validity-
//     filtered — regardless of the requested hops (documented ≤1-hop
//     fallback contract). Dgraph errors log a warn before degrading;
//     an EMPTY graph result is NOT an error and never falls back — it is
//     projection lag, which this read reports honestly as zero paths.
//
// Hydration happens once per call: all distinct reached ids go through a
// single batched mem.entities FINAL lookup so paths carry CH-native
// entity.Entity structs. Paths containing an id that fails hydration are
// dropped rather than rendered half-empty.
func (s *Service) Traverse(ctx context.Context, scope, rawKey string, entityType entity.Type, relation string, hops int) ([]Path, error) {
	scope = strings.TrimSpace(scope)
	if scope == "" {
		return nil, fmt.Errorf("%w: scope required", ErrInvalidInput)
	}
	key := strings.TrimSpace(rawKey)
	if key == "" {
		return nil, fmt.Errorf("%w: key required", ErrInvalidInput)
	}
	if relation != "" && !relationRe.MatchString(relation) {
		return nil, fmt.Errorf("%w: invalid relation %q", ErrInvalidInput, relation)
	}
	if hops < traverseHopsMin || hops > traverseHopsMax {
		return nil, fmt.Errorf("%w: hops must be within [%d,%d], got %d",
			ErrInvalidInput, traverseHopsMin, traverseHopsMax, hops)
	}

	norm, err := entity.Normalize(key)
	if err != nil {
		// Not entity-shaped ⇒ no row can exist; an honest miss, not an error.
		return []Path{}, nil
	}

	start, err := s.lookupTraverseStart(ctx, scope, entityType, norm.Key)
	if err != nil {
		return nil, err
	}
	if start == nil {
		return []Path{}, nil
	}

	if s.graph != nil {
		paths, err := s.traverseGraph(ctx, s.graph, scope, *start, relation, hops)
		if err != nil {
			s.log.Warn("memory: dgraph traverse failed; degrading to clickhouse 1-hop",
				"scope", scope,
				"entity_id", start.EntityID,
				"err", err)
		} else {
			return paths, nil
		}
	}
	return s.traverseFallback(ctx, scope, *start, relation)
}

// lookupTraverseStart resolves (scope, type, key) through the authoritative
// FINAL read path WITHOUT creating anything — the resolver's
// lookup-or-create would mint rows on read-path misses, so Enrich's plain
// SELECT pattern is mirrored here instead. nil = honest miss.
func (s *Service) lookupTraverseStart(ctx context.Context, scope string, entityType entity.Type, key string) (*entity.Entity, error) {
	var (
		entID     uuid.UUID
		firstSeen time.Time
		lastSeen  time.Time
		displayNm string
		attrs     map[string]string
	)
	err := s.conn.QueryRow(ctx,
		"SELECT entity_id, display_name, attrs, first_seen, last_seen "+
			"FROM mem.entities FINAL "+
			"WHERE scope = ? AND entity_type = ? AND key = ?",
		scope, string(entityType), key,
	).Scan(&entID, &displayNm, &attrs, &firstSeen, &lastSeen)
	if err != nil {
		// clickhouse-go signals an empty result with io.EOF on some paths
		// and sql.ErrNoRows on others; both mean "no such entity".
		if errors.Is(err, io.EOF) || errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("memory: traverse lookup %s/%s in scope %q: %w",
			entityType, key, scope, err)
	}
	return &entity.Entity{
		EntityID:    entID.String(),
		Scope:       scope,
		EntityType:  entityType,
		Key:         key,
		DisplayName: displayNm,
		Attrs:       attrs,
		FirstSeen:   firstSeen,
		LastSeen:    lastSeen,
	}, nil
}

// traverseGraph walks the Dgraph projection breadth-first from start.
// Each hop runs one batched DQL root over the whole frontier's ch_ids;
// edges are filtered in Go (scope equality, relation facet match, validity
// window) before a reached node joins the next frontier. The uid-keyed
// visited set makes every expansion terminate: a node can enter exactly
// one frontier, and parent links always connect consecutive levels, so
// walking parents strictly descends levels — no cycle can survive and no
// Path repeats a node.
func (s *Service) traverseGraph(ctx context.Context, g *graph.Store, scope string, start entity.Entity, relation string, hops int) ([]Path, error) {
	nodes := []walkNode{{chID: start.EntityID, parent: -1}}
	visited := map[string]bool{}
	frontier := []int{0}

	for depth := 1; depth <= hops && len(frontier) > 0 && len(nodes) <= maxTraversePaths; depth++ {
		literals := make([]string, len(frontier))
		for i, fi := range frontier {
			literals[i] = fmt.Sprintf("%q", nodes[fi].chID)
		}
		resp, err := g.Dgraph().NewReadOnlyTxn().Query(ctx,
			fmt.Sprintf(hopQueryTmpl, strings.Join(literals, ", ")))
		if err != nil {
			return nil, fmt.Errorf("memory: traverse dgraph hop %d: %w", depth, err)
		}
		var parsed struct {
			P []dgNode `json:"p"`
		}
		if err := json.Unmarshal(resp.Json, &parsed); err != nil {
			return nil, fmt.Errorf("memory: traverse decode hop %d response %s: %w", depth, resp.Json, err)
		}
		byChID := make(map[string]dgNode, len(parsed.P))
		for _, nd := range parsed.P {
			byChID[nd.ChID] = nd
		}

		// Level-1 integrity check: the start node must exist in the
		// projection AND belong to the request scope. A missing node is
		// projection lag — an honest empty result, never a fallback.
		if depth == 1 {
			root, ok := byChID[start.EntityID]
			if !ok || root.Scope != scope || root.Uid == "" {
				return []Path{}, nil
			}
			nodes[0].uid = root.Uid
			visited[root.Uid] = true
		}

		var next []int
		for _, fi := range frontier {
			src := nodes[fi]
			nd, ok := byChID[src.chID]
			if !ok || nd.Scope != scope {
				continue // defense-in-depth: never expand a foreign-scope node
			}
			for _, tgt := range nd.Out {
				if traverseEdgeOK(scope, relation, tgt) {
					if stop := appendWalkNode(&nodes, &next, visited, fi, tgt, src.chID); stop {
						break
					}
				}
			}
			for _, tgt := range nd.In {
				if traverseEdgeOK(scope, relation, tgt) {
					if stop := appendWalkNode(&nodes, &next, visited, fi, tgt, src.chID); stop {
						break
					}
				}
			}
			if len(nodes) > maxTraversePaths {
				break
			}
		}
		frontier = next
	}

	// Hydrate every reached id (start included) in ONE batched query so all
	// paths reference CH-native entities.
	ids := make([]uuid.UUID, 0, len(nodes))
	for _, nd := range nodes {
		if u, err := uuid.Parse(nd.chID); err == nil {
			ids = append(ids, u)
		}
	}
	byID, err := s.hydrateEntities(ctx, scope, ids)
	if err != nil {
		return nil, err
	}

	paths := make([]Path, 0, len(nodes)-1)
	for i := 1; i < len(nodes); i++ { // node 0 is the start: never an endpoint
		var chain []int
		for j := i; j >= 0; j = nodes[j].parent {
			chain = append(chain, j)
		}
		p := Path{Nodes: []entity.Entity{}, Relations: make([]string, 0, len(chain)-1)}
		complete := true
		for k := len(chain) - 1; k >= 0; k-- {
			e, ok := byID[nodes[chain[k]].chID]
			if !ok {
				complete = false // defensive: never render a half-hydrated path
				break
			}
			p.Nodes = append(p.Nodes, e)
			if k > 0 {
				p.Relations = append(p.Relations, nodes[chain[k-1]].rel)
			}
		}
		if complete {
			paths = append(paths, p)
		}
	}
	sortPaths(paths)
	return paths, nil
}

// appendWalkNode records one filtered edge target as a freshly discovered
// BFS node. It reports whether the path cap was hit.
func appendWalkNode(nodes *[]walkNode, next *[]int, visited map[string]bool, parentIdx int, tgt dgTarget, startChID string) bool {
	if tgt.Uid == "" || tgt.ChID == "" || tgt.ChID == startChID {
		return false
	}
	if visited[tgt.Uid] {
		return false // cycle terminator: each uid joins the walk at most once
	}
	if len(*nodes) >= maxTraversePaths+1 { // root + maxTraversePaths endpoints
		return true
	}
	visited[tgt.Uid] = true
	rel, _, _ := tgt.facets()
	*nodes = append(*nodes, walkNode{uid: tgt.Uid, chID: tgt.ChID, parent: parentIdx, rel: rel})
	*next = append(*next, len(*nodes)-1)
	return false
}

// traverseEdgeOK applies the read-side edge filters to one Dgraph edge:
// scope equality (defense-in-depth), relation-facet match when a filter is
// set, and the validity window against now. Facet datetimes arrive as
// RFC3339 strings; a PRESENT but unparseable value fails closed (edge
// skipped), while absent bounds default to open-ended — the projection
// always writes both, so absence only occurs on foreign data.
func traverseEdgeOK(scope, wantRel string, tgt dgTarget) bool {
	if tgt.Scope != scope || tgt.Uid == "" || tgt.ChID == "" {
		return false
	}
	rel, vfRaw, vtRaw := tgt.facets()
	if rel == "" || (wantRel != "" && rel != wantRel) {
		return false
	}
	now := time.Now().UTC()
	vf := time.Time{}
	if vfRaw != "" {
		t, err := time.Parse(time.RFC3339, vfRaw)
		if err != nil {
			return false
		}
		vf = t
	}
	vt := farFuture
	if vtRaw != "" {
		t, err := time.Parse(time.RFC3339, vtRaw)
		if err != nil {
			return false
		}
		vt = t
	}
	return !vf.After(now) && vt.After(now)
}

// sortPaths orders paths by terminal entity id so equal graphs yield equal
// responses regardless of Dgraph's row ordering.
func sortPaths(paths []Path) {
	for i := 1; i < len(paths); i++ { // insertion sort: tiny slices, stable
		for j := i; j > 0; j-- {
			a := paths[j-1].Nodes[len(paths[j-1].Nodes)-1].EntityID
			b := paths[j].Nodes[len(paths[j].Nodes)-1].EntityID
			if a <= b {
				break
			}
			paths[j-1], paths[j] = paths[j], paths[j-1]
		}
	}
}

// traverseFallback is the CH-only leg (design §6 "Graph DB down" posture):
// at most ONE hop through mem.edges, both directions, open edges only — the
// same query shape Enrich uses for neighbors. The ≤1-hop ceiling is a
// documented CONTRACT, not an accident: CH joins beyond one hop are
// quadratic and unbounded, while the graph engine exists precisely to make
// multi-hop cheap. A requested depth ≥2 therefore still returns a 1-hop
// result set. The cap is deterministic: distinct (nid, relation) pairs are
// ordered newest-edge-first with edge_id breaking ties, so a saturated
// LIMIT returns a stable subset.
func (s *Service) traverseFallback(ctx context.Context, scope string, start entity.Entity, relation string) ([]Path, error) {
	startU, err := uuid.Parse(start.EntityID)
	if err != nil {
		return nil, fmt.Errorf("memory: traverse fallback entity id %q: %w", start.EntityID, err)
	}
	q := "SELECT nid, relation FROM (" +
		"SELECT dst_id AS nid, relation, valid_from AS ts, edge_id AS ek FROM mem.edges " +
		"WHERE scope = ? AND src_id = ? AND valid_to > now64(3) " +
		"UNION ALL " +
		"SELECT src_id AS nid, relation, valid_from AS ts, edge_id AS ek FROM mem.edges " +
		"WHERE scope = ? AND dst_id = ? AND valid_to > now64(3)"
	args := []any{scope, startU, scope, startU}
	if relation != "" {
		q += " WHERE relation = ?"
		args = append(args, relation)
	}
	q += ") GROUP BY nid, relation " +
		fmt.Sprintf("ORDER BY max(ts) DESC, min(ek) ASC LIMIT %d", maxTraversePaths)

	rows, err := s.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("memory: traverse fallback neighbors %s: %w", start.EntityID, err)
	}
	defer rows.Close()

	type hop struct {
		nid uuid.UUID
		rel string
	}
	var hops []hop
	for rows.Next() {
		var h hop
		if err := rows.Scan(&h.nid, &h.rel); err != nil {
			return nil, fmt.Errorf("memory: traverse fallback scan %s: %w", start.EntityID, err)
		}
		hops = append(hops, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory: traverse fallback iterate %s: %w", start.EntityID, err)
	}

	ids := make([]uuid.UUID, 0, len(hops)+1)
	ids = append(ids, startU)
	for _, h := range hops {
		ids = append(ids, h.nid)
	}
	byID, err := s.hydrateEntities(ctx, scope, ids)
	if err != nil {
		return nil, err
	}

	paths := []Path{}
	for _, h := range hops {
		n, ok := byID[h.nid.String()]
		st, okStart := byID[startU.String()]
		if !ok || !okStart {
			continue // defensive: never render a half-hydrated path
		}
		paths = append(paths, Path{
			Nodes:     []entity.Entity{st, n},
			Relations: []string{h.rel},
		})
	}
	return paths, nil
}

// hydrateEntities resolves entity ids to full entities through the
// authoritative read path (FINAL, scope-filtered) in ONE round trip —
// mirroring Enrich's hydration so Traverse output stays CH-native.
func (s *Service) hydrateEntities(ctx context.Context, scope string, ids []uuid.UUID) (map[string]entity.Entity, error) {
	out := make(map[string]entity.Entity, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	ph := strings.TrimSuffix(strings.Repeat("toUUID(?), ", len(ids)), ", ")
	args := make([]any, 0, len(ids)+1)
	args = append(args, scope)
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := s.conn.Query(ctx,
		"SELECT entity_id, entity_type, key, display_name, attrs, first_seen, last_seen "+
			"FROM mem.entities FINAL WHERE scope = ? AND entity_id IN ("+ph+")",
		args...)
	if err != nil {
		return nil, fmt.Errorf("memory: traverse hydrate %d entities in scope %q: %w", len(ids), scope, err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			e        entity.Entity
			typ      string // Enum8 does not scan into named string types
			entID    uuid.UUID
			displayN string
			attrs    map[string]string
		)
		if err := rows.Scan(&entID, &typ, &e.Key, &displayN, &attrs, &e.FirstSeen, &e.LastSeen); err != nil {
			return nil, fmt.Errorf("memory: traverse scan hydrated entity: %w", err)
		}
		e.EntityID = entID.String()
		e.EntityType = entity.Type(typ)
		e.DisplayName = displayN
		e.Attrs = attrs
		e.Scope = scope
		out[e.EntityID] = e
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory: traverse iterate hydrated entities: %w", err)
	}
	return out, nil
}
