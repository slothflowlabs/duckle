# Flow control samples

Runnable examples for the `ctl.*` components - the parts of a pipeline that
decide *when* and *whether* work happens rather than what the SQL computes.
Each pipeline reads the committed `data/orders.csv` and writes under
`output/`, so nothing external is needed.

- **route_orders** - `ctl.switch` routes rows by branch expression
  (`reject` / `large` / `default`), `ctl.die` guards the reject branch
  (fires only when it has rows), `ctl.log` and `ctl.warn` write run-log
  lines on the way to the sinks.
- **foreach_region** - `ctl.wait` delays, `xf.distinct` reduces to one row
  per region, `ctl.foreach` runs `_region_export` once per row with
  `${ITER_ITEM_REGION}` substituted into the child's filter and sink path.
- **try_fallback** - `ctl.try` installs `_notify_error` as the fallback
  pipeline: if any stage after it fails, the child runs before the error
  surfaces.

Child pipelines are prefixed `_` and are not meant to run on their own -
they are referenced by `pipelineRef` / `fallbackPipelineRef`. Note that
`_notify_error` is never executed in this workspace: `try_fallback`
succeeds, and `ctl.try` only fires its fallback on a later-stage failure,
so the child is compile-checked by `validate` and nothing more.

Run one headlessly:

```sh
duckle-runner --pipeline examples/flow-control/pipelines/route_orders.pipeline.json \
    --workspace examples/flow-control
```

CI validates every example pipeline on all platforms and runs the
top-level ones on Linux (see `.github/workflows/ci.yml`).
