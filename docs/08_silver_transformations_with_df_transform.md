# Silver Transformations with df.transform()

The silver notebook should not contain one giant block of transformation code.

Instead:
- define reusable transformation functions
- import them from `src/transformations`
- chain them with `df.transform(...)`

Suggested transformation pattern:
- standardize columns
- filter invalid rows
- add business flags
- apply selected custom logic

This teaches stronger pipeline design.
