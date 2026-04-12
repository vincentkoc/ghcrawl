---
title: "Clustering optimization: dedupe_summary embeddings with vectorlite HNSW outperform exact kNN"
date: 2026-03-30
category: performance-issues
module: clustering
problem_type: performance_issue
component: tooling
symptoms:
  - "Original exact kNN clustering produced ungoverned 455-member mega-clusters"
  - "Only 31.2% of threads ended up in multi-member clusters"
  - "Raw title+body embeddings included template boilerplate noise"
root_cause: missing_tooling
resolution_type: tooling_addition
severity: medium
tags:
  - clustering
  - embeddings
  - vectorlite
  - hnsw
  - llm-as-judge
  - prompt-optimization
  - dedupe-summary
---

# Clustering optimization: dedupe_summary embeddings with vectorlite HNSW outperform exact kNN

## Problem

GitCrawl clusters ~18,500 GitHub issues/PRs by embedding similarity to identify duplicates and related threads. The original pipeline used exact k-nearest-neighbor search on title+body embeddings with unbounded clustering. This produced 4.73/5 coherence but ungoverned cluster growth (max 455 members), only 31.2% coverage, and 1.70% outlier rate. The goal was to improve coherence, control cluster sizes, and increase coverage.

## Provenance

- Project: [pwrdrvr/ghcrawl](https://github.com/pwrdrvr/ghcrawl)
- Recovered source session: `b82c1142-949a-433d-9486-df441f2d36bd`
- Entry point: `/ce-optimize`
- Notes: the prompts below were cleaned up for spelling, grammar, and readability while preserving the original intent

### Sample Kickoff Prompt

Short version:

```text
We want Vectorlite ANN plus post-processing to completely replace the in-memory exact kNN search.

We are only getting about 18% cluster membership for the ~18k open issues and PRs in the default home-directory database. We want to run an optimization that uses that database as a starting point but does not modify the shared copy.

The working model should be:
- copy the database for the run
- convert the copy to use Vectorlite as the base for each test
- make a per-experiment copy when needed
- discard those copies afterward, or keep cluster population artifacts without embeddings for later analysis
```

Expanded version:

```text
We have about 18k open issues and PRs. We want to test several changes that improve clustering quality, reduce the count of singleton clusters, and improve the quality of matches within each cluster.

We do not need clusters to mean "exactly the same issue" across all issues and PRs. We want useful, slightly broader clusters that let us extract the superset of repeated reports. We care more about grouping related reports together than about splitting clusters because one item mentions a narrower sub-point.

Use the shared default database only as input. Do not mutate it. Each experiment should work from its own copied database so we can compare runs safely and keep useful artifacts for later analysis.
```

### Early Signal From The First Sweep

This was the first useful threshold sweep recovered from the optimization thread before later judge-driven refinements:

| Experiment | minScore | Multi-member % | Max cluster size | Edges |
|---|---:|---:|---:|---:|
| Baseline | 0.82 | 31.4% | 619 | 6,934 |
| Exp 2 | 0.80 | 42.0% | 2,417 | 10,744 |
| Exp 3 | 0.78 | 53.2% | 4,905 | 16,400 |
| Exp 1 | 0.75 | 68.6% | 9,780 | 29,157 |

The immediate takeaway from the optimize loop was that lowering the threshold increased coverage fast, but Union-Find transitive closure created degenerate mega-clusters. That pushed the work toward bounded clustering, refinement, and eventually judge-scored evaluation.

## Approach

### Phase 1: Summarization Prompt Optimization

Tested 11 prompt variants for summarizing issue/PR content before embedding. An LLM-as-judge (gpt-5-mini) scored each on boilerplate removal, signal density, and clustering suitability (1-5 scale).

**Winner: `v5-component-focused` (4.97/5 vs baseline 2.65/5).** Key insight: explicit component-first structure (e.g., "Discord gateway: connection drops on resume") clusters far better than generic summaries. Full summarization of 18.5k threads cost $26 using gpt-5.4-mini.

### Phase 2: Clustering Experiments

Sixteen configurations tested across four dimensions:

- **Embedding sources**: title, body, dedupe_summary (optimized summaries), and combinations
- **Search backend**: Vectorlite HNSW approximate nearest-neighbor vs exact kNN
- **Score aggregation**: max, mean, weighted, min-of-2, boost
- **Parameters**: similarity threshold (0.75-0.88), neighbor count k (6, 12), max cluster size (200, 400)

Clustering used **size-bounded Union-Find**: edges sorted by descending score, merges refused when exceeding maxSize cap.

**Evaluation** used LLM-as-judge with stratified sampling: 30 clusters (10 large, 10 mid, 10 small) scored for coherence, plus 15 singletons scored for false-negative detection.

## Results

### Baselines

| Configuration | Sources | Backend | Mode | Coherence | Multi% | MaxSz | AvgSz | Outlier% | Duration |
|---|---|---|---|---|---|---|---|---|---|
| **Original** | title+body | exact kNN | basic (unbounded) | 4.73 | 31.2% | 455 | 3.52 | 1.70% | 800s |
| All sources, max agg | all 3 | vectorlite | bounded (200) | 4.62 | 49.5% | 200 | 4.64 | 2.07% | 180s |

### Key Experiments (vectorlite HNSW, bounded mode, maxSize=200)

| Experiment | Sources | Aggregation | Threshold | Coherence | Multi% | Outlier% | Takeaway |
|---|---|---|---|---|---|---|---|
| **source-dedupe-only** | dedupe_summary | max | 0.82 | **4.93** | 44.6% | **0.85%** | **Recommended.** Best coherence at reasonable coverage. |
| agg-min-of-2 | all 3 | min-of-2 | 0.82 | 4.97 | 23.8% | 0.67% | Highest coherence but low coverage. Precision champion. |
| param-high-threshold | all 3 | max | 0.88 | 5.00 | 14.4% | 0.00% | Perfect coherence, too conservative for general use. |
| agg-boost | all 3 | boost | 0.82 | 4.85 | 49.5% | 0.94% | Best multi-source option if more coverage needed. |
| source-body-dedupe | body+dedupe | max | 0.82 | 4.89 | 48.8% | 1.47% | Adding body helps coverage slightly, hurts coherence. |
| param-low-threshold | dedupe_summary | max | 0.75 | 4.77 | 77.4% | 0.96% | High coverage but coherence drops and clusters get large (avg 11.7). |
| baseline-all-max | all 3 | max | 0.82 | 4.62 | 49.5% | 2.07% | Adding dedupe_summary to max agg made things *worse*. |

### What the Columns Mean

- **Coherence** (1-5): LLM judge score for how well cluster members relate. Stratified sample of 30 clusters.
- **Multi%**: Percentage of threads in multi-member clusters (coverage).
- **Outlier%**: Percentage of cluster members judged as not belonging.

## Recommended Configuration

**`source-dedupe-only` with Vectorlite HNSW, threshold 0.82, maxSize 200.**

- **+0.20 coherence** over original (4.93 vs 4.73)
- **+13% coverage** (44.6% vs 31.2% multi-member)
- **Half the outlier rate** (0.85% vs 1.70%)
- **15x faster** (55s vs 800s)
- **Simplest**: single embedding source, no aggregation complexity
- **Controlled cluster sizes**: max 200 vs unbounded 455

## Key Learnings

1. **Summarization prompt quality is the biggest lever.** The prompt improvement (2.65 to 4.97 judge score) drove more quality gain than any clustering algorithm change. Good embeddings matter more than clever aggregation.

2. **More sources does not mean better clusters.** Naive multi-source max aggregation (4.62) was *worse* than single-source dedupe-summary (4.93). Title and body embeddings introduce noise that dilutes the optimized summary signal.

3. **Multi-source only helps with strict aggregation.** The only multi-source configs that beat single-source used min-of-2 or high thresholds -- essentially filtering out noise from weaker sources. Added complexity for marginal gain.

4. **HNSW approximate search outperforms exact kNN in practice.** The approximate search found ~2x more edges because it casts a wider net. This produced better clusters, not worse, while being 15x faster.

5. **Size-bounded Union-Find is essential.** The original system's largest cluster (455 members) was incoherent. Capping at 200 with score-ordered merging ensures best edges are used first.

6. **Mid-range and small clusters are consistently perfect (5.0).** Quality issues concentrate in the largest clusters. The top_by_size bucket is the discriminator between configs.

## Future Work

- **Threshold tuning per component**: Different areas may cluster at different similarity levels
- **Hierarchical clustering**: Tight clusters first (0.88), then looser grouping (0.78) for topic organization
- **Coverage gap analysis**: 55.4% of threads remain singletons -- sampling these would quantify false-negative rate
- **Incremental updates**: Delta-based matching against existing cluster centroids instead of full rebuild

## Related

- `docs/DESIGN.md` -- Original architecture describing exact cosine similarity kNN approach
- `docs/PLAN.md` -- Phase 4 (Embeddings) and Phase 5 (OpenSearch Evaluation)
- `.context/compound-engineering/ce-optimize/embedding-clustering/` -- Raw experiment results (16 JSON files)
- `.context/compound-engineering/ce-optimize/summary-prompt/` -- Prompt optimization results (11 variants)
