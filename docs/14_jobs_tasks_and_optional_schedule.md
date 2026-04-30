# Jobs, Tasks, and Optional Schedule

Day 4 is not mainly a Jobs day, but a light operational layer should still be included.

Create one Job with tasks such as:
1. setup
2. bronze ingestion
3. silver transformations
4. gold outputs
5. governance / validation

Widgets may be used for selected notebook inputs.

`dbutils.jobs.taskValues` may be used once for a small workflow value such as a row count.

A simple schedule can be added as an optional final step.
