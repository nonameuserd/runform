# Golden delivery demo (inspectable session artifacts)

This directory contains a **checked-in snapshot** of `.akc/delivery/<delivery_id>/` sidecars so operators can inspect delivery state in `akc view` without running `akc deliver`.

Try it:

```bash
cd examples/golden-delivery-demo
akc view tui
# or
akc view web --out-dir ./.akc/viewer/web-demo
```

What you’re looking at:

- `out/demo/delivery-repo/.akc/delivery/deliv-demo-1/request.json`
- `out/demo/delivery-repo/.akc/delivery/deliv-demo-1/session.json`
- `out/demo/delivery-repo/.akc/delivery/deliv-demo-1/events.json`
- `out/demo/delivery-repo/.akc/delivery/deliv-demo-1/recipients.json`

These are **schema-valid** delivery artifacts (`delivery_*` v1) and are meant to support the “inspectable evidence” story.

