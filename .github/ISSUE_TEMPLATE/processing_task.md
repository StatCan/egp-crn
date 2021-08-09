---
name: Processing Task
about: Create a task for data processing
title: 'Process <source> <year>'
labels: processing
assignees: ''

---

**Description of tasks**
Process <source> <year> data for release as an NRN product.
- [ ] update field mapping yaml(s)
- [ ] process <source> <year> data
- [ ] update release notes and sphinx documentation
  - [ ] copy updated yamls to `src/stage_5/distribution_docs`
  - [ ] copy updated rsts to `docs/source`
  - [ ] build updated Sphinx documentation via command: `sphinx-build -b html nrn-rrn/docs/source nrn-rrn/docs/_build`
- [ ] copy data to server
- [ ] confirm WMS updates and publication to Open Maps
- [ ] custom task: ...
